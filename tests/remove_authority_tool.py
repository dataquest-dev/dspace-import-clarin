"""Import the remove_authority tool without leaving its logging setup behind.

Like every tool in this repository it configures file logging on the root logger at
import time. A test run must not inherit that, so the handlers it adds are taken off
again and the empty log file it created is removed.
"""
import importlib.util
import logging
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TOOL_FILE = os.path.join(ROOT_DIR, "tools", "remove_authority", "remove_authority.py")

_tool = None


def load_tool():
    global _tool
    if _tool is not None:
        return _tool

    root = logging.getLogger()
    known, level = list(root.handlers), root.level

    # test_bitstream_no_retry puts a stub `dspace` package in sys.modules, drop it
    # so the tool imports the real one whatever order the tests run in
    for name in [n for n in list(sys.modules) if n.split(".")[0] == "dspace"]:
        del sys.modules[name]

    spec = importlib.util.spec_from_file_location("remove_authority", TOOL_FILE)
    module = importlib.util.module_from_spec(spec)
    sys.modules["remove_authority"] = module
    spec.loader.exec_module(module)

    for handler in list(root.handlers):
        if handler not in known:
            root.removeHandler(handler)
            handler.close()
    root.setLevel(level)
    # without a handler logging falls back to lastResort, which prints to stderr
    root.addHandler(logging.NullHandler())

    log_file = module.env["log_file"]
    if os.path.exists(log_file) and os.path.getsize(log_file) == 0:
        os.remove(log_file)

    _tool = module
    return module
