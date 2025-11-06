import pexpect  # Added for pseudo-terminal support
import subprocess  # Re-added as requested
from concurrent.futures import ThreadPoolExecutor
import threading
import os
import csv
import json
import logging
import sys
import signal
import time
import shutil
import queue  # Added for thread-safe message propagation
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from typing import List, Optional, Dict, Tuple

# Load environment variables from .env file
load_dotenv()

# Set up global configuration for SFTP downloader
sftp_config = {
    "base_url": "sftp://fms.biosino.org:44398/",
    "output_dir": "/data35/OEP00000860",
    "max_connections": 10,
    "log_file": "sftp_download_log.txt",
    "state_file": "download_state_aria2.json"
}

# Set up global logging for high-level messages (console and file)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),  # Output to console
        logging.FileHandler(sftp_config["log_file"])  # Output to sftp_download_log.txt
    ]
)

# Directory for per-thread logs
LOG_DIR = "/data35/logs"

# Base Downloader class
class Downloader(ABC):
    """Abstract base class for file downloaders."""

    def __init__(
        self,
        output_dir: str,
        max_connections: int = 10,
        log_file: Optional[str] = "download_log.txt",
        state_file: str = "download_state_aria2.json"
    ):
        """
        Initialize the Downloader with output directory and configuration.

        Args:
            output_dir (str): Directory where downloaded files will be saved.
            max_connections (int): Maximum number of concurrent connections.
            log_file (Optional[str]): Path to the global log file.
            state_file (str): Path to the state file tracking download progress.
        """
        self.output_dir = output_dir
        self.max_connections = max_connections
        self.state_file = state_file
        self.state_lock = threading.Lock()  # Lock for thread-safe state updates
        self.last_backup_time = 0  # Timestamp of the last state file backup
        self.backup_interval = 3600  # Backup interval in seconds (1 hour)
        self.stop_event = threading.Event()  # Event to signal download interruption
        self._thread_local = threading.local()  # Thread-local storage for loggers
        self.logger = logging.getLogger(self.__class__.__name__)  # Global logger for high-level messages
        self.message_queue = queue.Queue()  # Queue for propagating messages to main thread

    @abstractmethod
    def _build_command(self, url: str, checksum: Optional[str] = None, run_id: Optional[str] = None) -> List[str]:
        """Build the command to download a file (to be implemented by subclasses)."""
        pass

    def _get_thread_logger(self, start_time: float) -> logging.Logger:
        """
        Create or retrieve a logger for the current thread.

        Args:
            start_time (float): Start time of the download in seconds since epoch.

        Returns:
            logging.Logger: Thread-specific logger.
        """
        if not hasattr(self._thread_local, "logger"):
            # Construct unique log file name: <timestamp>_<thread_name>.log
            thread_name = threading.current_thread().name
            log_filename = f"{int(start_time)}_{thread_name}.log"
            log_path = os.path.join(LOG_DIR, log_filename)
            
            # Configure logger for this thread
            thread_logger = logging.getLogger(f"ThreadLogger_{int(start_time)}_{thread_name}")
            thread_logger.setLevel(logging.INFO)
            thread_handler = logging.FileHandler(log_path)
            thread_handler.setFormatter(logging.Formatter("%(asctime)s [%(threadName)s] %(message)s"))
            thread_logger.handlers = [thread_handler]  # Replace any existing handlers
            thread_logger.propagate = False  # Prevent propagation to global logger
            self._thread_local.logger = thread_logger
        
        return self._thread_local.logger

    def download(self, url: str, checksum: Optional[str] = None, run_id: Optional[str] = None) -> Tuple[bool, str, float]:
        """
        Download a single file using the configured downloader with pexpect for real-time output.

        Args:
            url (str): URL of the file to download.
            checksum (Optional[str]): Expected MD5 checksum for verification.
            run_id (Optional[str]): Run ID to organize output directory.

        Returns:
            Tuple[bool, str, float]: Success status, result message, and duration in seconds.
        """
        if self.stop_event.is_set():
            return False, "Stopped", 0.0

        filename = os.path.basename(url)
        filepath = os.path.join(self.output_dir, run_id or "", filename) if run_id else os.path.join(self.output_dir, filename)
        command = self._build_command(url, checksum, run_id)
        
        start_time = time.time()  # Record start time in seconds since epoch
        thread_logger = self._get_thread_logger(start_time)
        
        # Propagate "starting" message to main thread with URL
        thread_name = threading.current_thread().name
        self.message_queue.put(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Thread {thread_name} started downloading {filename} from {url}")
        thread_logger.info(f"Starting download of {url}")
        
        # Spawn aria2c in a pseudo-terminal with pexpect for real-time output
        process = pexpect.spawn(" ".join(command), encoding="utf-8", timeout=None)
        self._thread_local.process = process

        # Read output in real-time from the pseudo-terminal with a timeout
        while process.isalive() and not self.stop_event.is_set():
            try:
                # Use a short timeout to allow checking stop_event frequently
                line = process.read_nonblocking(size=1024, timeout=1).strip()
                if line:
                    thread_logger.info(line)
                    thread_logger.handlers[0].flush()  # Force immediate write to log file
            except pexpect.TIMEOUT:
                continue  # No new output yet, keep looping
            except pexpect.EOF:
                break  # Process has finished outputting

        # Ensure process termination if interrupted
        if self.stop_event.is_set() and process.isalive():
            process.close(force=True)  # Forcefully terminate if interrupted

        # Wait for process to complete and get exit status
        if process.isalive():
            process.wait()
        end_time = time.time()
        duration = end_time - start_time

        success = process.exitstatus == 0
        verification_result = "Checksum verified" if success and checksum else "Download failed" if not success else "No checksum provided"
        thread_logger.info(f"Download {'completed' if success else 'failed'} in {duration:.2f}s: {verification_result}")
        thread_logger.handlers[0].flush()  # Ensure final message is written
        return success, verification_result, duration

    def _update_state(self, filename: str, success: bool, checksum: Optional[str], filepath: str, verification_result: str) -> None:
        """
        Update the download state file with the result of a single download, creating a backup every hour.

        Args:
            filename (str): Name of the downloaded file.
            success (bool): Whether the download succeeded.
            checksum (Optional[str]): MD5 checksum used for verification.
            filepath (str): Path where the file was saved.
            verification_result (str): Result message from the download.
        """
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S.%f")
        current_time = time.time()
        
        # Define state based on download outcome
        if success:
            state = {
                "status": "completed",
                "timestamp": timestamp,
                "md5": checksum or "",
                "path": filepath,
                "tool": "aria2",
                "verified_with_md5": bool(checksum),
                "checksum_valid": True
            }
        else:
            state = {
                "status": "failed",
                "timestamp": timestamp,
                "md5": checksum or "",
                "path": filepath,
                "tool": "aria2",
                "verified_with_md5": False,
                "checksum_valid": False,
                "error": verification_result
            }

        # Thread-safe state file update with backup
        with self.state_lock:
            current_state = {}
            if os.path.isfile(self.state_file):
                with open(self.state_file, "r") as f:
                    try:
                        current_state = json.load(f)
                    except json.JSONDecodeError:
                        self.logger.warning(f"State file {self.state_file} corrupted; attempting to recover from backup")
                        backup_file = f"{self.state_file}.bak"
                        if os.path.isfile(backup_file):
                            shutil.copy2(backup_file, self.state_file)
                            with open(self.state_file, "r") as f:
                                try:
                                    current_state = json.load(f)
                                    self.logger.info(f"Recovered state from {backup_file}")
                                except json.JSONDecodeError:
                                    self.logger.error(f"Backup file {backup_file} also corrupted; starting fresh")
                                    current_state = {}
                        else:
                            self.logger.error(f"No backup available; resetting {self.state_file}")
                            current_state = {}

            # Create a backup of the state file every hour
            if current_time - self.last_backup_time >= self.backup_interval and os.path.isfile(self.state_file):
                backup_file = f"{self.state_file}.bak"
                try:
                    shutil.copy2(self.state_file, backup_file)
                    self.last_backup_time = current_time
                    self.logger.info(f"Created backup: {backup_file}")
                except Exception as e:
                    self.logger.error(f"Failed to create backup {backup_file}: {e}")

            current_state[filename] = state
            with open(self.state_file, "w") as f:
                json.dump(current_state, f, indent=4)

    def download_files(
        self,
        urls: List[str],
        checksums: Optional[Dict[str, str]] = None,
        filename_to_run_id: Optional[Dict[str, str]] = None,
        max_threads: int = 5
    ) -> Dict[str, Tuple[bool, str, float]]:
        """
        Download multiple files concurrently.

        Args:
            urls (List[str]): List of URLs to download.
            checksums (Optional[Dict[str, str]]): Dictionary of filenames to MD5 checksums.
            filename_to_run_id (Optional[Dict[str, str]]): Mapping of filenames to run IDs.
            max_threads (int): Maximum number of concurrent threads.

        Returns:
            Dict[str, Tuple[bool, str, float]]: Results of each download (success, message, duration).
        """
        self.logger.info(f"Starting downloads with max {max_threads} threads...")
        results = {}
        checksums = checksums or {}
        filename_to_run_id = filename_to_run_id or {}

        # Load existing state to skip completed downloads
        current_state = {}
        with self.state_lock:
            if os.path.isfile(self.state_file):
                with open(self.state_file, "r") as f:
                    try:
                        current_state = json.load(f)
                    except json.JSONDecodeError:
                        self.logger.warning(f"State file {self.state_file} corrupted; will recover on next update")

        # Filter URLs to download only uncompleted files
        urls_to_download = []
        for url in urls:
            filename = os.path.basename(url)
            state = current_state.get(filename, {})
            if not (
                state.get("status") == "completed" and
                state.get("verified_with_md5", False) and
                state.get("checksum_valid", False)
            ):
                urls_to_download.append(url)
            else:
                self.logger.info(f"Skipping {filename}: already downloaded and verified")

        if not urls_to_download:
            self.logger.info("No files to download; all are already completed and verified")
            return results

        # Start a thread to print messages from the queue
        def print_queue_messages():
            while not self.stop_event.is_set() or not self.message_queue.empty():
                try:
                    message = self.message_queue.get(timeout=1)
                    print(message)
                    self.message_queue.task_done()
                except queue.Empty:
                    continue

        message_thread = threading.Thread(target=print_queue_messages, daemon=True)
        message_thread.start()

        start_total = time.perf_counter()
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {
                executor.submit(
                    self.download,
                    url,
                    checksums.get(os.path.basename(url)),
                    filename_to_run_id.get(os.path.basename(url))
                ): url
                for url in urls_to_download
            }
            for future in futures:
                url = futures[future]
                filename = os.path.basename(url)
                run_id = filename_to_run_id.get(filename)
                filepath = os.path.join(self.output_dir, run_id or "", filename) if run_id else os.path.join(self.output_dir, filename)
                try:
                    success, verification_result, duration = future.result()
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Thread handling {filename} finished: {'Success' if success else 'Failed'} - {verification_result}")
                    results[filename] = (success, verification_result, duration)
                    self._update_state(filename, success, checksums.get(filename), filepath, verification_result)
                except Exception as e:
                    self.logger.error(f"{filename}: Exception occurred - {e}")
                    results[filename] = (False, f"Exception: {e}", 0.0)
                    self._update_state(filename, False, checksums.get(filename), filepath, f"Exception: {e}")

        total_duration = time.perf_counter() - start_total
        self.stop_event.set()
        message_thread.join()  # Wait for message thread to finish
        self.logger.info(f"All downloads completed or interrupted in {total_duration:.2f}s!")
        return results

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit, ensuring cleanup."""
        self.stop_event.set()
        self.handle_signal(None, None)  # Ensure all processes are terminated

    def handle_signal(self, sig, frame):
        """Handle interrupt signals (e.g., Ctrl+C) to stop downloads gracefully."""
        self.logger.info("Interrupt received, stopping downloads...")
        self.stop_event.set()
        # Terminate all pexpect processes
        with threading.Lock():  # Ensure thread-safe access to thread-local storage
            for thread in threading.enumerate():
                if hasattr(self._thread_local, "process") and self._thread_local.process:
                    if self._thread_local.process.isalive():
                        self._thread_local.process.close(force=True)  # Forcefully terminate pexpect process
                    del self._thread_local.process  # Clean up reference

# SFTP Downloader using aria2c
class SFTPDownloader(Downloader):
    """Downloader implementation using aria2c for SFTP."""

    def __init__(
        self,
        base_url: str,
        output_dir: str,
        max_connections: int = 10,
        log_file: Optional[str] = "download_log.txt",
        state_file: str = "download_state_aria2.json"
    ):
        """
        Initialize the SFTPDownloader.

        Args:
            base_url (str): Base URL for SFTP downloads.
            output_dir (str): Directory where files will be saved.
            max_connections (int): Maximum concurrent connections.
            log_file (Optional[str]): Path to the global log file.
            state_file (str): Path to the state file.
        """
        super().__init__(output_dir, max_connections, log_file, state_file)
        self.base_url = base_url.rstrip("/")
        self.username = os.getenv("SFTP_USER")
        self.password = os.getenv("SFTP_PASS")
        if not self.username or not self.password:
            raise ValueError("SFTP_USER and SFTP_PASS must be set in the environment or .env file")

    def _build_command(self, url: str, checksum: Optional[str] = None, run_id: Optional[str] = None) -> List[str]:
        """
        Build the aria2c command for downloading a file.

        Args:
            url (str): URL of the file to download.
            checksum (Optional[str]): MD5 checksum for verification.
            run_id (Optional[str]): Run ID for organizing output.

        Returns:
            List[str]: Command list for subprocess.Popen or pexpect.spawn.
        """
        output_dir = os.path.join(self.output_dir, run_id or "") if run_id else self.output_dir
        filename = os.path.basename(url)
        log_file = os.path.join(output_dir, f"{filename}.log")
        
        # Ensure directories exist for output and aria2c log
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        cmd = [
            "aria2c",
            "--ftp-user", self.username,
            "--ftp-passwd", self.password,
            "-x", str(self.max_connections),
            "--dir", output_dir,
            "--console-log-level=notice",
            "--summary-interval=60",
            "--show-console-readout=true",
            "--log", log_file,  # aria2c log file
            "--log-level=info"
        ]
        if checksum:
            cmd.extend(["--checksum", f"md5={checksum}"])
        cmd.append(url)
        return cmd

# Class to handle Biosino.org metadata file parsing
class BiosinoOrgMetadataFile:
    """Class to parse and manage metadata from Biosino.org download link files."""
    
    def __init__(self, file_path: str):
        """
        Initialize with the path to the metadata file.
        
        Args:
            file_path (str): Path to the tab-delimited metadata file.
        
        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If required columns are missing.
        """
        self.file_path = file_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self._validate_file()

    def _validate_file(self) -> None:
        """Validate that the file exists and has required columns."""
        if not os.path.isfile(self.file_path):
            raise FileNotFoundError(f"Metadata file not found: {self.file_path}")
        
        with open(self.file_path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            required_columns = {"run_id", "ftp_file_path", "fileName", "MD5"}
            if not required_columns.issubset(reader.fieldnames):
                missing = required_columns - set(reader.fieldnames)
                raise ValueError(f"Metadata file missing required columns: {missing}")

    def parse(self, base_url: str) -> Tuple[List[str], Dict[str, str], Dict[str, str]]:
        """
        Parse the metadata file, sort by run_id, and return URLs, checksums, and filename-to-run_id mapping.
        
        Args:
            base_url (str): Base URL for constructing full SFTP URLs.
        
        Returns:
            Tuple[List[str], Dict[str, str], Dict[str, str]]: 
                - Sorted list of URLs
                - Dictionary of filename to MD5 checksums
                - Dictionary of filename to run_id
        
        Raises:
            csv.Error: If the file is malformed.
        """
        rows = []
        try:
            with open(self.file_path, "r") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    rows.append(row)
        except csv.Error as e:
            raise csv.Error(f"Error parsing {self.file_path}: {e}")

        sorted_rows = sorted(rows, key=lambda x: x["run_id"])
        urls = []
        checksums = {}
        filename_to_run_id = {}
        for row in sorted_rows:
            ftp_path = row["ftp_file_path"]
            url = f"{base_url.rstrip('/')}{ftp_path}"
            urls.append(url)
            checksums[row["fileName"]] = row["MD5"]
            filename_to_run_id[row["fileName"]] = row["run_id"]
        
        return urls, checksums, filename_to_run_id

    def update_download_state(self, state_file: str = "download_state.json", output_file: str = "download_state_aria2.json") -> None:
        """
        Parse a download state JSON file and update/output a new state file with verification, writing after each row.
        
        Args:
            state_file (str): Path to the input download state JSON file.
            output_file (str): Path to the output download state JSON file.
        
        Raises:
            FileNotFoundError: If the input state file or metadata file is missing.
            json.JSONDecodeError: If the state file is malformed.
        """
        if not os.path.isfile(state_file):
            raise FileNotFoundError(f"Download state file not found: {state_file}")

        with open(state_file, "r") as f:
            try:
                old_state = json.load(f)
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(f"Error parsing {state_file}: {e}", e.doc, e.pos)

        new_state = {}
        if os.path.isfile(output_file):
            with open(output_file, "r") as f:
                try:
                    new_state = json.load(f)
                except json.JSONDecodeError:
                    new_state = {}

        _, metadata_checksums, filename_to_run_id = self.parse("sftp://fms.biosino.org:44398/")
        sorted_items = sorted(old_state.items(), key=lambda x: filename_to_run_id.get(x[0], ""))

        for filename, data in sorted_items:
            try:
                self.logger.info(f"Starting processing of {filename}")
                row_new = new_state.get(filename, {})
                
                if row_new.get("verified_with_md5", False):
                    continue

                if data.get("status") == "completed":
                    file_path = data.get("path")
                    expected_md5 = metadata_checksums.get(filename)
                    
                    if file_path and os.path.isfile(file_path) and expected_md5:
                        self.logger.info(f"Verifying checksum for {filename} at {file_path}")
                        result = subprocess.run(["md5sum", file_path], capture_output=True, text=True)
                        if result.returncode == 0:
                            computed_md5 = result.stdout.split()[0]
                            is_valid = computed_md5 == expected_md5
                            new_state[filename] = {
                                **data,
                                "tool": "lftp",
                                "verified_with_md5": True,
                                "checksum_valid": is_valid
                            }
                        else:
                            new_state[filename] = {
                                **data,
                                "tool": "lftp",
                                "verified_with_md5": True,
                                "checksum_valid": False
                            }
                    else:
                        new_state[filename] = {
                            **data,
                            "tool": "lftp",
                            "verified_with_md5": False,
                            "checksum_valid": False
                        }
                else:
                    new_state[filename] = {
                        **data,
                        "tool": "lftp",
                        "verified_with_md5": False,
                        "checksum_valid": False
                    }

                with open(output_file, "w") as f:
                    json.dump(new_state, f, indent=4)

            except Exception as e:
                self.logger.error(f"Error processing {filename} in {state_file}: {e}")
                continue

# Main execution
if __name__ == "__main__":
    """Main entry point for the script."""
    os.makedirs(LOG_DIR, exist_ok=True)  # Moved here from global scope
    metadata_file = "project_OEP00000860_data_download_link.txt"
    metadata = BiosinoOrgMetadataFile(metadata_file)

    # Update download state before starting downloads
    metadata.update_download_state("download_state.json", "download_state_aria2.json")
    
    # Parse metadata for URLs, checksums, and run_ids
    urls, checksums, filename_to_run_id = metadata.parse(sftp_config["base_url"])

    # Start downloading with SFTPDownloader
    with SFTPDownloader(**sftp_config) as sftp_downloader:
        # Set up signal handlers for graceful interruption
        signal.signal(signal.SIGINT, lambda sig, frame: sftp_downloader.handle_signal(sig, frame))
        signal.signal(signal.SIGTERM, lambda sig, frame: sftp_downloader.handle_signal(sig, frame))
        
        # Execute downloads and collect results, using max_connections as max_threads
        results = sftp_downloader.download_files(
            urls,
            checksums=checksums,
            filename_to_run_id=filename_to_run_id,
            max_threads=sftp_config["max_connections"]
        )
        
        # Print results to console
        for filename, (success, result, duration) in results.items():
            print(f"{filename}: {'Success' if success else 'Failed'} - {result} - {duration:.2f}s")