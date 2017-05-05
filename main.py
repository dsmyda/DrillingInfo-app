"""

@author: Danny Smyda
@date: 1-31-2016

"""

import os
import sys

from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.uic import loadUi

import t_factory
import math
from master import Master
from states import abbrev

CHECK_MARK = u'\u2713'
PARTITION_SIZE = 25000

class MainWindow(QMainWindow):

    def __init__(self, parent=None):
        super(MainWindow, self).__init__(parent)
        loadUi(os.getcwd() + os.sep +'UI' + os.sep + 'DrillingInfo.ui', self)

        #================= Download tab signals =============================
        self.findChild(QPushButton, "prodm_browse").clicked\
            .connect(lambda: self.saveDir("prodm_lineEdit"))
        self.findChild(QPushButton, "save_browse").clicked\
            .connect(lambda: self.saveDir("save_lineEdit"))
        self.findChild(QPushButton, "download_button").clicked\
            .connect(self.download)
        self.findChild(QComboBox, "type_comboBox").activated.connect\
            (self.ifSelected)

        #Initializations
        self.findChild(QComboBox, "state_comboBox").addItems([item.value for item in abbrev])
        self.findChild(QComboBox, "type_comboBox").addItems\
            (["Permits", "Production Headers", "Production Monthly", "Earthquake Data"])
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
        self.findChild(QPushButton, "clearSelected").clicked\
            .connect(self.clear_selected)
        self.findChild(QPushButton, "pauseSelected").clicked\
            .connect(self.pause_selected)
        self.findChild(QPushButton, "resumeSelected").clicked\
            .connect(self.resume_selected)
        self.findChild(QPushButton, "shutdown_pushButton").clicked\
            .connect(self.shutdown_selected)
        self.findChild(QPushButton, "queueSelected").clicked\
            .connect(self.queue_selected)
        self.statusTable = self.findChild(QTableWidget, "statusTable")
        self.findChild(QPushButton, "increasePriority").clicked\
            .connect(self.increase_priority)
        #====================================================================
        self.master = Master()
        self.id_map = {}
        self.init_DI_view()
        self.statusBar().showMessage('Ready.')

    def init_DI_view(self):
        self.findChild(QPushButton, "prodm_browse").hide()
        self.findChild(QLabel, "prodLabel").hide()
        self.findChild(QLineEdit, "prodm_lineEdit").hide()
        self.findChild(QDateEdit, "start_dateEdit").hide()
        self.findChild(QDateEdit, "end_dateEdit").hide()
        self.findChild(QLabel, "startLabel").hide()
        self.findChild(QLabel, "endLabel").hide()
        self.findChild(QLabel, "prodm_choose").hide()
    def get_id(self, name):
        return self.id_map[name]

    def ifSelected(self):
        type = self.findChild(QComboBox, "type_comboBox")
        if type.currentText() == "Production Monthly":
            self.init_DI_view()
            self.findChild(QPushButton, "prodm_browse").show()
            self.findChild(QLabel, "prodm_choose").show()
            self.findChild(QLabel, "prodLabel").show()
            self.findChild(QLineEdit, "prodm_lineEdit").show()
        if type.currentText() == "Production Headers" or \
                type.currentText() == "Permits":
            self.init_DI_view()
        elif type.currentText() == "Earthquake Data":
            self.findChild(QLineEdit, "api_lineEdit").hide()
            self.findChild(QPushButton, "prodm_browse").hide()
            self.findChild(QLabel, "prodLabel").hide()
            self.findChild(QLabel, "prodm_choose").hide()
            self.findChild(QLineEdit, "prodm_lineEdit").hide()
            self.findChild(QLabel, "apiLabel").hide()
            self.findChild(QDateEdit, "start_dateEdit").show()
            self.findChild(QDateEdit, "end_dateEdit").show()
            self.findChild(QLabel, "startLabel").show()
            self.findChild(QLabel, "endLabel").show()

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
        src = self.findChild(QLineEdit, "originalfile_lineEdit").text()
        dest = self.findChild(QLineEdit, "saveConvert_lineEdit").text()
        format = self.findChild(QComboBox, "format_comboBox").text()

        #TODO make sure the file name converting into does not exist.

        t = t_factory.get_convert_instance(format, src, dest)
        self.master.send(t)

    def download(self):
        """ Upon download button press, retrieve the text from
        the fields and pass them to the worker factory. """

        # ================ Gather necessary fields ==============================
        save_fp = self.findChild(QLineEdit, "save_lineEdit").text()
        prodm_fp = self.findChild(QLineEdit, "prodm_lineEdit").text()
        api_key =self.findChild(QLineEdit, "api_lineEdit").text()
        type = self.findChild(QComboBox, "type_comboBox").currentText()
        start_page = 1
        page_size = 100
        state = self.findChild(QComboBox, "state_comboBox").currentText()
        save_id = None
#        auto_convert = self.findChild(QCheckBox, "dtaCheckbox").isChecked()
        # =======================================================================

        prodm_fp += "/"+abbrev(state).name+"prodh.csv"

        if self.master.init_save(state, type, save_fp):
            reply = QMessageBox.question(self, "Continue?", "A file of this type was found in the provided save directory."
                                         +" Would you like to continue from where it left off on .... (WARNING) Significant"
                                          " time differences between pulls can cause data inconsistencies.",
                                         QMessageBox.Yes, QMessageBox.No)
            if reply == QMessageBox.Yes:
                start_page = self.master.get_save_page()
                page_size = self.master.get_save_size()
                if type == "Production Monthly":
                    save_id = self.master.get_save_id()

        if type == "Production Monthly":
            partitioned_fs = self.master.split_nd_cpy(prodm_fp, os.getcwd()+ os.sep + "tmp"+os.sep+"tmp_prodh", PARTITION_SIZE)
            for part_fs in partitioned_fs:
                t = t_factory.get_api_instance(type, state, api_key, start_page, page_size, save_fp, part_fs, save_id, self.statusTable.rowCount())
                t._trigger.connect(self.dynamic_refresh)
                self.id_map[str(t)] = id(t)
                self.add_to_table(str(t))
                self.master.send(t)
        else:
            t = t_factory.get_api_instance(type, state, api_key, start_page, page_size, save_fp, prodm_fp, save_id, self.statusTable.rowCount())
            t._trigger.connect(self.dynamic_refresh)
            self.id_map[str(t)] = id(t)
            self.add_to_table(str(t))
            self.master.send(t)

    def add_to_table(self, pid_str):
        self.cleanTable(pid_str)
        rowPosition = self.statusTable.rowCount()
        self.statusTable.insertRow(rowPosition)

        self.statusTable.setItem(rowPosition, 0, self.table_text())
        for x in range(1, self.statusTable.columnCount()):
            self.statusTable.setItem(rowPosition, x, self.check_text())

        self.statusTable.item(rowPosition, 0).setText(pid_str)
        s = 4 if self.master.status(self.get_id(pid_str)) == -1 else self.master.status(self.get_id(pid_str))
        self.statusTable.item(rowPosition, s).setText(CHECK_MARK + " 0%")

    def cleanTable(self, worker_name):
        for pos in range(0, self.statusTable.rowCount()):
            if self.statusTable.item(pos, 0).text() == worker_name:
                self.statusTable.removeRow(pos)

    def closeEvent(self, QCloseEvent):
        #Safely log and shutdown pids for exit.
        if self.master.active_cnt() > 0:
            reply = QMessageBox.question(self, "Quit", "Are you sure you want to quit? Processes are still active.",
                                         QMessageBox.Yes, QMessageBox.No)
            if reply == QMessageBox.Yes:
                QCloseEvent.accept()
                self.master.exit()
            else:
                QCloseEvent.ignore()
        else:
            #Call just to be sure.
            self.master.exit()

    def dynamic_refresh(self, data):
        data_split = data.split("/")
        worker_name = data_split[0]
        progress = data_split[1]
        pos = int(data_split[2])
        self.clearCurrentStatus(pos)
        s = 4 if self.master.status(self.get_id(worker_name)) == -1 else self.master.status(self.get_id(worker_name))
        self.statusTable.item(pos, s)\
                .setText(CHECK_MARK + " " + progress)

    def increase_priority(self):
        idx = self.statusTable.selectionModel().selectedRows()
        for i in idx:
            self.master.increase_priority(self.get_id(self.statusTable.item(i.row(), 0).text()))

    def pause_selected(self):
        idx = self.statusTable.selectionModel().selectedRows()
        for i in idx:
            self.master.pause(self.get_id(self.statusTable.item(i.row(), 0).text()))

    def clear_selected(self):
        idx = self.statusTable.selectionModel().selectedRows()
        for i in idx:
            if self.master.status(self.get_id(self.statusTable.item(i.row(), 0).text())) == -1:
                self.statusTable.removeRow(i.row())

    def resume_selected(self):
        indx = self.statusTable.selectionModel().selectedRows()
        for i in indx:
            self.master.resume(self.get_id(self.statusTable.item(i.row(), 0).text()))

    def shutdown_selected(self):
        indx = self.statusTable.selectionModel().selectedRows()
        for i in indx:
            self.master.shutdown(self.get_id(self.statusTable.item(i.row(), 0).text()))

    def queue_selected(self):
        indx = self.statusTable.selectionModel().selectedRows()
        for i in indx:
            self.master.queue(self.get_id(self.statusTable.item(i.row(), 0).text()))

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

    def integrity_check(self):
        if self.master.app_crash():
            continue_choice = QMessageBox.question(self, 'System Recovery',
                                               "Looks like the application previously crashed. "+
                                               "Would you like to continue where you left off?",
                                                   QMessageBox.Yes, QMessageBox.No)
            if continue_choice == QMessageBox.Yes:
                self.master.app_recovery(self)
        self.master.log("^", extra={"descriptor": "", "id": "", "pagenum": "", "pagesize": ""})

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    window.integrity_check()
    sys.exit(app.exec_())