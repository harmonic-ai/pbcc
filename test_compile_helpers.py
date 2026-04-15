import unittest
from unittest.mock import patch

from pbcc.compile import cxx_command, extension_module_suffix, path_to_module_name, python_config_candidates


class CompileHelperTests(unittest.TestCase):
    def test_path_to_module_name_handles_posix_paths(self) -> None:
        self.assertEqual(path_to_module_name("pbcc/test.proto"), "pbcc.test")

    def test_path_to_module_name_handles_windows_paths(self) -> None:
        self.assertEqual(path_to_module_name(r"pbcc\test.proto"), "pbcc.test")

    def test_path_to_module_name_strips_current_directory_prefix(self) -> None:
        self.assertEqual(path_to_module_name("./generated/test_pbcc"), "generated.test_pbcc")

    def test_extension_module_suffix_uses_python_config(self) -> None:
        with patch("pbcc.compile.sysconfig.get_config_var", return_value=".cpython-313-x86_64-linux-gnu.so"):
            self.assertEqual(extension_module_suffix(), ".cpython-313-x86_64-linux-gnu.so")

    def test_extension_module_suffix_falls_back_to_so(self) -> None:
        with patch("pbcc.compile.sysconfig.get_config_var", return_value=None):
            self.assertEqual(extension_module_suffix(), ".so")

    def test_python_config_candidates_prefers_explicit_env_override(self) -> None:
        with (
            patch.dict("os.environ", {"PYTHON_CONFIG": r"C:\tools\python3-config.exe"}, clear=False),
            patch("pbcc.compile.sysconfig.get_config_var", return_value=None),
            patch("pbcc.compile.os.path.realpath", return_value=r"C:\Python314\python.exe"),
            patch("pbcc.compile.os.path.exists", return_value=False),
        ):
            self.assertEqual(python_config_candidates()[0], r"C:\tools\python3-config.exe")

    def test_cxx_command_prefers_pbcc_override(self) -> None:
        with (
            patch.dict("os.environ", {"PBCC_CXX": r"C:\llvm\bin\clang++.exe"}, clear=False),
            patch("pbcc.compile.shutil.which", return_value=None),
        ):
            self.assertEqual(cxx_command(), r"C:\llvm\bin\clang++.exe")


if __name__ == "__main__":
    unittest.main()
