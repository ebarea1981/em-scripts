#!/usr/bin/env python3

import unittest
import tempfile
import os
import shutil
import re
from move_files_recursively import move_files_recursively


class TestMoveFilesRecursively(unittest.TestCase):
    def setUp(self):
        """Set up temporary directories for testing"""
        self.temp_dir = tempfile.mkdtemp()
        self.source_dir = os.path.join(self.temp_dir, 'source')
        self.dest_dir = os.path.join(self.temp_dir, 'dest')
        os.makedirs(self.source_dir)
        os.makedirs(self.dest_dir)

    def tearDown(self):
        """Clean up temporary directories"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_test_files(self, file_list):
        """Helper to create test files in source directory"""
        for file_path, content in file_list:
            full_path = os.path.join(self.source_dir, file_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, 'w') as f:
                f.write(content)

    def get_files_in_dir(self, directory):
        """Helper to get all files recursively in a directory"""
        files = []
        for root, dirs, filenames in os.walk(directory):
            for filename in filenames:
                rel_path = os.path.relpath(os.path.join(root, filename), directory)
                # Normalize path separators for cross-platform compatibility
                files.append(rel_path.replace(os.sep, '/'))
        return sorted(files)

    def test_basic_file_moving(self):
        """Test basic functionality - move files with simple pattern"""
        # Create test files
        test_files = [
            ('file1.txt', 'content1'),
            ('file2.txt', 'content2'),
            ('file3.log', 'content3'),
            ('subdir/file4.txt', 'content4'),
        ]
        self.create_test_files(test_files)

        # Move .txt files
        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        # Check results
        source_files = self.get_files_in_dir(self.source_dir)
        dest_files = self.get_files_in_dir(self.dest_dir)

        # Should have moved .txt files to dest, kept .log in source
        self.assertEqual(source_files, ['file3.log'])
        self.assertEqual(sorted(dest_files), ['file1.txt', 'file2.txt', 'subdir/file4.txt'])

        # Check content is preserved
        with open(os.path.join(self.dest_dir, 'file1.txt'), 'r') as f:
            self.assertEqual(f.read(), 'content1')

    def test_no_files_match_pattern(self):
        """Test when no files match the pattern"""
        test_files = [
            ('file1.txt', 'content1'),
            ('file2.log', 'content2'),
        ]
        self.create_test_files(test_files)

        # Try to move .jpg files (none exist)
        pattern = r'.*\.jpg$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        # All files should remain in source, dest should be empty
        source_files = self.get_files_in_dir(self.source_dir)
        dest_files = self.get_files_in_dir(self.dest_dir)

        self.assertEqual(sorted(source_files), ['file1.txt', 'file2.log'])
        self.assertEqual(dest_files, [])

    def test_all_files_match_pattern(self):
        """Test when all files match the pattern"""
        test_files = [
            ('file1.txt', 'content1'),
            ('file2.txt', 'content2'),
            ('subdir/file3.txt', 'content3'),
        ]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        source_files = self.get_files_in_dir(self.source_dir)
        dest_files = self.get_files_in_dir(self.dest_dir)

        self.assertEqual(source_files, [])
        self.assertEqual(sorted(dest_files), ['file1.txt', 'file2.txt', 'subdir/file3.txt'])

    def test_empty_source_directory(self):
        """Test moving from empty source directory"""
        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        source_files = self.get_files_in_dir(self.source_dir)
        dest_files = self.get_files_in_dir(self.dest_dir)

        self.assertEqual(source_files, [])
        self.assertEqual(dest_files, [])

    def test_filename_conflicts(self):
        """Test handling of filename conflicts"""
        test_files = [
            ('file1.txt', 'content1'),
            ('subdir/file1.txt', 'content2'),  # Same filename in different subdir
        ]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        source_files = self.get_files_in_dir(self.source_dir)
        dest_files = self.get_files_in_dir(self.dest_dir)

        self.assertEqual(source_files, [])
        # Should have both files in their respective directories (no conflict since different paths)
        self.assertEqual(sorted(dest_files), ['file1.txt', 'subdir/file1.txt'])

    def test_multiple_filename_conflicts(self):
        """Test handling of multiple filename conflicts"""
        test_files = [
            ('file1.txt', 'content1'),
            ('subdir1/file1.txt', 'content2'),
            ('subdir2/file1.txt', 'content3'),
            ('subdir3/file1.txt', 'content4'),
        ]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        dest_files = self.get_files_in_dir(self.dest_dir)

        # All files should be preserved in their respective directories (no conflicts)
        self.assertEqual(sorted(dest_files), ['file1.txt', 'subdir1/file1.txt', 'subdir2/file1.txt', 'subdir3/file1.txt'])

    def test_destination_directory_creation(self):
        """Test that destination directory is created if it doesn't exist"""
        non_existent_dest = os.path.join(self.temp_dir, 'new_dest')

        test_files = [('file1.txt', 'content1')]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, non_existent_dest, pattern)

        # Check destination was created and file moved
        self.assertTrue(os.path.exists(non_existent_dest))
        dest_files = self.get_files_in_dir(non_existent_dest)
        self.assertEqual(dest_files, ['file1.txt'])

    def test_wildcard_patterns(self):
        """Test various wildcard and regex patterns"""
        test_files = [
            ('test.txt', 'content1'),
            ('test.log', 'content2'),
            ('backup.bak', 'content3'),
            ('data.txt.bak', 'content4'),
            ('temp.tmp', 'content5'),
        ]
        self.create_test_files(test_files)

        # Test wildcard pattern
        pattern = r'.*\.bak$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        dest_files = self.get_files_in_dir(self.dest_dir)
        self.assertEqual(sorted(dest_files), ['backup.bak', 'data.txt.bak'])

    def test_complex_regex_patterns(self):
        """Test complex regex patterns"""
        test_files = [
            ('file001.txt', 'content1'),
            ('file002.txt', 'content2'),
            ('file100.txt', 'content3'),
            ('other.txt', 'content4'),
        ]
        self.create_test_files(test_files)

        # Pattern to match files with 3 digits
        pattern = r'.*\d{3}\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        dest_files = self.get_files_in_dir(self.dest_dir)
        self.assertEqual(sorted(dest_files), ['file001.txt', 'file002.txt', 'file100.txt'])

    def test_hidden_files(self):
        """Test moving hidden files (starting with .)"""
        test_files = [
            ('.hidden.txt', 'content1'),
            ('normal.txt', 'content2'),
            ('subdir/.hidden.log', 'content3'),
        ]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        dest_files = self.get_files_in_dir(self.dest_dir)
        source_files = self.get_files_in_dir(self.source_dir)

        self.assertEqual(sorted(dest_files), ['.hidden.txt', 'normal.txt'])
        self.assertEqual(source_files, ['subdir/.hidden.log'])

    def test_deep_directory_structure(self):
        """Test moving files from deeply nested directories"""
        test_files = [
            ('level1/level2/level3/deep_file.txt', 'content1'),
            ('level1/other.txt', 'content2'),
            ('root.txt', 'content3'),
        ]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        dest_files = self.get_files_in_dir(self.dest_dir)
        source_files = self.get_files_in_dir(self.source_dir)

        # All .txt files should be moved to dest preserving directory structure
        self.assertEqual(sorted(dest_files), ['level1/level2/level3/deep_file.txt', 'level1/other.txt', 'root.txt'])
        self.assertEqual(source_files, [])

    def test_files_with_special_characters(self):
        """Test files with special characters in names"""
        test_files = [
            ('file with spaces.txt', 'content1'),
            ('file-with-dashes.txt', 'content2'),
            ('file_with_underscores.txt', 'content3'),
            ('file(1).txt', 'content4'),
            ('file[1].txt', 'content5'),
        ]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        dest_files = self.get_files_in_dir(self.dest_dir)
        self.assertEqual(len(dest_files), 5)
        self.assertIn('file with spaces.txt', dest_files)
        self.assertIn('file-with-dashes.txt', dest_files)
        self.assertIn('file_with_underscores.txt', dest_files)

    def test_actual_filename_conflicts(self):
        """Test conflict resolution when files would overwrite existing destination files"""
        # First, create and move some files
        test_files = [('test.txt', 'content1')]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        # Now create a new file with the same name in source
        with open(os.path.join(self.source_dir, 'test.txt'), 'w') as f:
            f.write('content2')

        # Move again - this should create test_1.txt to avoid conflict
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        dest_files = self.get_files_in_dir(self.dest_dir)
        self.assertEqual(sorted(dest_files), ['test.txt', 'test_1.txt'])

    def test_invalid_regex_pattern(self):
        """Test behavior with invalid regex pattern"""
        test_files = [('file1.txt', 'content1')]
        self.create_test_files(test_files)

        # Invalid regex should raise exception
        with self.assertRaises(re.error):
            move_files_recursively(self.source_dir, self.dest_dir, r'[invalid')

    def test_same_source_and_destination(self):
        """Test moving files to the same directory"""
        test_files = [('file1.txt', 'content1')]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        # This creates a conflict - file gets renamed to avoid overwriting itself
        move_files_recursively(self.source_dir, self.source_dir, pattern)

        source_files = self.get_files_in_dir(self.source_dir)
        # File gets renamed due to conflict resolution
        self.assertEqual(source_files, ['file1_1.txt'])

    def test_case_sensitive_patterns(self):
        """Test case-sensitive pattern matching"""
        # Use filenames that are truly different on Windows filesystem
        test_files = [
            ('file_lower.txt', 'content1'),
            ('FILE_UPPER.TXT', 'content2'),
            ('File_Mixed.txt', 'content3'),
        ]
        self.create_test_files(test_files)

        # Pattern is case-sensitive by default - only matches lowercase .txt
        pattern = r'.*\.txt$'  # matches .txt (lowercase)
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        dest_files = self.get_files_in_dir(self.dest_dir)
        source_files = self.get_files_in_dir(self.source_dir)

        # 'file_lower.txt' and 'File_Mixed.txt' should match (.txt lowercase), 'FILE_UPPER.TXT' should not
        self.assertEqual(sorted(dest_files), ['File_Mixed.txt', 'file_lower.txt'])
        self.assertEqual(source_files, ['FILE_UPPER.TXT'])

    def test_zero_length_files(self):
        """Test moving empty files"""
        test_files = [
            ('empty.txt', ''),
            ('nonempty.txt', 'content'),
        ]
        self.create_test_files(test_files)

        pattern = r'.*\.txt$'
        move_files_recursively(self.source_dir, self.dest_dir, pattern)

        dest_files = self.get_files_in_dir(self.dest_dir)

        self.assertEqual(sorted(dest_files), ['empty.txt', 'nonempty.txt'])

        # Check empty file is still empty
        with open(os.path.join(self.dest_dir, 'empty.txt'), 'r') as f:
            self.assertEqual(f.read(), '')


if __name__ == '__main__':
    unittest.main()
