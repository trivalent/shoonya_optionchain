#!/usr/bin/env python
from PySide6.QtWidgets import QApplication

from shoonya_win import ShoonyaWindow
if __name__ == '__main__':

    import sys

    app = QApplication(sys.argv)
    shoonya_window = ShoonyaWindow()
    shoonya_window.show()
    sys.exit(app.exec())
