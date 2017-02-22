"""

@author: Danny Smyda
@date: 1-31-2016

"""

import sys

from PyQt5.QtWidgets import *
from PyQt5.uic import loadUi
from PyQt5.QtCore import *
from PyQt5.QtGui import *
import worker_factory
import master
from states import abbrev

CHECK_MARK = u'\u2713'

class MainWindow(QMainWindow):

    def __init__(self, parent=None):
        super(MainWindow, self).__init__(parent)
        loadUi('/home/dsmyda/DrillingInfo.ui', self)

        #================= Download tab signals =============================
        self.findChild(QPushButton, "prodm_browse").clicked\
            .connect(lambda: self.getFiles("prodm_lineEdit", filter="(*.json)"))
        self.findChild(QPushButton, "save_browse").clicked\
            .connect(lambda: self.saveDir("save_lineEdit"))
        self.findChild(QPushButton, "download_button").clicked\
            .connect(self.download)

        #Initializations
        self.findChild(QComboBox, "state_comboBox").addItems([item.value for item in abbrev])
        self.findChild(QComboBox, "type_comboBox").addItems\
            (["Permits", "Production Headers", "Production Monthly"])
        # ===================================================================

        #================= Convert tab signals ==============================
        self.findChild(QPushButton, "originalfile_browse").clicked\
            .connect(lambda: self.getFiles("originalfile_lineEdit", filter="(*.json *.csv)"))
        self.findChild(QPushButton, "saveConvert_button").clicked\
            .connect(lambda: self.saveDir("saveConvert_lineEdit"))
        self.findChild(QPushButton, "convert_pushButton").clicked\
            .connect(self.convert)
        #====================================================================

        #================ Activity tab signals ==============================
        self.findChild(QPushButton, "refresh_pushButton").clicked\
            .connect(self.refresh)
        self.statusTable = self.findChild(QTableWidget, "statusTable")
        #====================================================================

        self.statusBar().showMessage('Ready.')

    def getFiles(self, textEdit, filter="All Files (*)"):
        dlg = QFileDialog()
        dlg.setNameFilter(filter)
        if dlg.exec_():
            filename = dlg.selectedFiles()
            self.findChild(QLineEdit, textEdit).clear()
            self.findChild(QLineEdit, textEdit).insert(filename[0])

    def saveDir(self, textEdit):
        dlg = QFileDialog()
        dlg.setFileMode(QFileDialog.DirectoryOnly)
        if dlg.exec_():
            filename = dlg.selectedFiles()
            self.findChild(QLineEdit, textEdit).clear()
            self.findChild(QLineEdit, textEdit).insert(filename[0])

    def convert(self):
        pass

    def download(self):
        """ Upon download button press, retrieve the text from
        the fields and pass them to the worker factory. """

        # ================ Gather necessary fields ==============================
        save_fp = self.findChild(QLineEdit, "save_lineEdit").text()
        prodm_fp = self.findChild(QLineEdit, "prodm_lineEdit").text()
        api_key =self.findChild(QLineEdit, "api_lineEdit").text()
        type = self.findChild(QComboBox, "type_comboBox").currentText()
        start_page = int(self.findChild(QLineEdit, "startpage_lineEdit").text())
        page_size = int(self.findChild(QLineEdit, "pagesize_lineEdit").text())
        state = self.findChild(QComboBox, "state_comboBox").currentText()
        format = self.findChild(QComboBox, "format_comboBox_dl").currentText()
        # =======================================================================

        pid = worker_factory.get_instance(type, state, api_key, start_page, page_size, format, save_fp, prodm_fp)
        master.send(pid)

        # ================ Update table and reset ==============================
        rowPosition = self.statusTable.rowCount()
        self.statusTable.insertRow(rowPosition)

        self.statusTable.setItem(rowPosition, 0, self.table_text())
        for x in range(1, self.statusTable.columnCount()):
            self.statusTable.setItem(rowPosition, x, self.check_text())

        self.statusTable.item(rowPosition, 0).setText(str(pid))
        self.statusTable.item(rowPosition, master.get_status(str(pid))).setText(CHECK_MARK)

        self.findChild(QLineEdit, "prodm_lineEdit").clear()
        # ======================================================================


    def refresh(self):
        for pos in range(0, self.statusTable.rowCount()):
            self.clearCurrentStatus(pos)
            self.statusTable.item(pos, master.get_status(self.statusTable.item(pos, 0).text())).setText(CHECK_MARK)

    def clearCurrentStatus(self, x):
        for y in range(1, self.statusTable.columnCount()):
            self.statusTable.item(x,y).setText("")

    def table_text(self):
        widget = QTableWidgetItem("")
        widget.setTextAlignment(Qt.AlignCenter)
        widget.setFont(QFont("Times", 10))

        return widget

    def check_text(self):
        widget = QTableWidgetItem("")
        widget.setTextAlignment(Qt.AlignCenter)
        widget.setFont(QFont("Times", 16))

        return widget

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())