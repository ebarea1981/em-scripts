import json
import os
import subprocess
import threading
import time
import queue
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional, Tuple


class VerifyMD5:
    def __init__(self, state_file_path: str, max_threads: int = 4, tool: str = "lftp"):
        """
        Initialize the MD5 verification class.
        
        Args:
            state_file_path: Path to the JSON file that tracks file status
            max_threads: Maximum number of threads to run in parallel
            tool: Tool name to record in the state file
        """
        self.state_file_path = state_file_path
        self.max_threads = max_threads
        self.tool = tool
        self.state_lock = threading.Lock()
        self._load_state()
        
    def _load_state(self) -> None:
        """Load existing state from the state file or initialize empty state."""
        try:
            if os.path.exists(self.state_file_path):
                with open(self.state_file_path, 'r') as f:
                    self.state = json.load(f)
            else:
                self.state = {}
        except Exception as e:
            print(f"Error loading state file: {e}")
            self.state = {}
    
    def _save_state(self) -> None:
        """Save current state to the state file."""
        with open(self.state_file_path, 'w') as f:
            json.dump(self.state, f, indent=4)
    
    def load_checksum_file(self, checksum_file_path: str) -> Dict[str, str]:
        """
        Load MD5 checksums from a file.
        
        Args:
            checksum_file_path: Path to the MD5 checksum file
            
        Returns:
            Dictionary mapping filenames to expected MD5 checksums
        """
        checksums = {}
        try:
            with open(checksum_file_path, 'r') as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        # Format is typically: <md5sum> <filename>
                        md5sum = parts[0]
                        filename = parts[1]
                        if filename.startswith('*'):  # Some md5sum outputs prepend * for binary mode
                            filename = filename[1:]
                        checksums[filename] = md5sum
            return checksums
        except Exception as e:
            print(f"Error loading checksum file: {e}")
            return {}
    
    def verify_file(self, file_path: str, expected_md5: str) -> Tuple[bool, str]:
        """
        Verify a single file using md5sum.
        
        Args:
            file_path: Path to the file to verify
            expected_md5: Expected MD5 checksum
            
        Returns:
            Tuple of (is_valid, actual_md5)
        """
        try:
            result = subprocess.run(['md5sum', file_path], 
                                    capture_output=True, 
                                    text=True, 
                                    check=True)
            actual_md5 = result.stdout.split()[0]
            return actual_md5.lower() == expected_md5.lower(), actual_md5
        except subprocess.SubprocessError as e:
            print(f"Error running md5sum on {file_path}: {e}")
            return False, ""
    
    def _verify_file_thread(self, filename: str, file_path: str, expected_md5: str, 
                           message_queue: queue.Queue) -> None:
        """
        Thread function to verify a file and update the state.
        
        Args:
            filename: Base filename (for the state file)
            file_path: Full path to the file
            expected_md5: Expected MD5 checksum
            message_queue: Queue for sending messages to the main thread
        """
        thread_id = threading.get_native_id()
        
        # Send starting message to the main thread
        message_queue.put(f"Thread {thread_id}: Starting verification of {filename}")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        
        valid, actual_md5 = self.verify_file(file_path, expected_md5)
        
        status = "completed" if valid else "failed"
        
        # Update state file with results
        with self.state_lock:
            self.state[filename] = {
                "status": status,
                "timestamp": timestamp,
                "md5": actual_md5 if actual_md5 else expected_md5,
                "path": file_path,
                "verified_with_md5": True,
                "checksum_valid": valid,
                "tool": self.tool
            }
            self._save_state()
        
        # Send completion message to the main thread
        message_queue.put(f"Thread {thread_id}: Finished verification of {filename}. Valid: {valid}")
    
    def _message_reporter(self, message_queue: queue.Queue, stop_event: threading.Event) -> None:
        """
        Thread function to report messages from the worker threads.
        
        Args:
            message_queue: Queue containing messages from worker threads
            stop_event: Event to signal when to stop reporting
        """
        while not stop_event.is_set() or not message_queue.empty():
            try:
                message = message_queue.get(timeout=0.1)
                print(message)
                message_queue.task_done()
            except queue.Empty:
                continue
    
    def verify_from_checksum_file(self, checksum_file_path: str, base_dir: Optional[str] = None) -> None:
        """
        Load a checksum file and verify all files in it using parallel threads.
        
        Args:
            checksum_file_path: Path to the MD5 checksum file
            base_dir: Optional base directory where files are located
        """
        # Load the checksums from the file
        checksums = self.load_checksum_file(checksum_file_path)
        
        if not checksums:
            print(f"No checksums found in file: {checksum_file_path}")
            return
        
        print(f"Loaded {len(checksums)} checksums from {checksum_file_path}")
        
        # Create a message queue for thread communication
        message_queue = queue.Queue()
        
        # Create a stop event for the message reporter
        stop_event = threading.Event()
        
        # Start the message reporter thread
        reporter_thread = threading.Thread(
            target=self._message_reporter,
            args=(message_queue, stop_event),
            daemon=True
        )
        reporter_thread.start()
        
        # Process all files in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = []
            
            for filename, expected_md5 in checksums.items():
                if base_dir:
                    file_path = os.path.join(base_dir, filename)
                else:
                    file_path = filename
                
                if not os.path.exists(file_path):
                    message_queue.put(f"File not found: {file_path}")
                    continue
                
                futures.append(
                    executor.submit(
                        self._verify_file_thread, 
                        filename, 
                        file_path, 
                        expected_md5,
                        message_queue
                    )
                )
            
            # Wait for all verifications to complete
            concurrent.futures.wait(futures)
        
        # Signal the reporter thread to stop and wait for it to finish
        stop_event.set()
        reporter_thread.join()
        
        print(f"All verifications completed. Results saved to {self.state_file_path}")
