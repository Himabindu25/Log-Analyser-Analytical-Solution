from PyQt5.QtWidgets import (QWidget, QPushButton, QLineEdit, QVBoxLayout,
                             QInputDialog, QApplication, QFileDialog)
from PyQt5.QtGui import QPalette

from PyQt5.QtCore import Qt

from Database_Operations import DatabaseOperations
from Analyse_Logs import AnalyseLogs

# from Log_Enabler import logEnabler

import sys

class Example(QWidget):

    def __init__(self):
        super().__init__()

        self.initUI()

    def initUI(self):

        self.btn = QPushButton('Upload Logs', self)
        self.btn.move(20, 20)
        self.btn.clicked.connect(self.getfiles)
        self.le = QLineEdit(self)
        self.le.move(130, 22)
        self.setGeometry(300, 300, 450, 350)

        button = QPushButton('Knowledge/Abend Builder')
        button.setToolTip('Generate Legacy Knowledge Excel sheets ')
        button.clicked.connect(self.on_click_K)

        button2 = QPushButton('Graph Analyzer')
        button2.setToolTip('Generate Graphs ')
        button2.clicked.connect(self.on_click_L)

        layout = QVBoxLayout()
        layout.addWidget(button)
        layout.addWidget(button2)
        self.setLayout(layout)

        self.setWindowTitle('Job Log Explorer')
        self.show()

    def getfiles(self):
        options = QFileDialog.Options()
        options = QFileDialog.DontUseNativeDialog
        fileName, _ = QFileDialog.getOpenFileName(None, "QFileDialog.getOpenFileName()", "",
                                                  "All Files (*);;Text Files (*.txt)", options=options)
        # text1 = fileName
        print(fileName)
        # if fileName:
        #    print(fileName)
        # self.textBrowser.setText(fileName)

        try:

            insert_str, f_name = db.file_content(fileName)

            print('Inserted into Database: ' + str(db.db_insert(insert_str, f_name)))

        except Exception as e:

            print('Error: {}.\nFile path not found.'.format(e))

    def on_click_K(self):
        print('K button click')

        cursor = db.read_all_rec()
        for records in cursor:

            ext_file_name1 = records['file name']
            print(ext_file_name1)

          #  count += 1
          #  print('\n' + str(count) + '. ' + ext_file_name)

            data_list = db.read_by_field(ext_file_name1)

            log.extract_keyword(data_list)
            log.extarct_steps(data_list)
            log.extract_utility(data_list)
            log.extract_condcode(data_list)
            log.extract_lib(data_list)
            log.step_division(data_list)
            log.extract_abend(data_list)
            log.extract_reason(data_list)
            log.extract_abend_info(data_list)
            log.extract_job_info(data_list)

        cursor.close()  # Close Cursor

    def on_click_L(self):
        print('L button click')
        # enable = logEnabler()
        dep_spec = int(self.ui.depth_spec.value())
        # selected_directory7 = QFileDialog.getExistingDirectory()

        # if not selected_directory7:
        #     return
        # print(selected_directory7)


    def showDialog(self):
        text, ok = QInputDialog.getText(self, 'Log Explorer',
                                        'Enter path to logs:')

        if ok:
            self.le.setText(str(text))

def main():
    app = QApplication(sys.argv)
    palette = QPalette()
    palette.setColor(QPalette.ButtonText, Qt.red)
    app.setPalette(palette)
    ex = Example()
    sys.exit(app.exec_())


if __name__ == '__main__':
    # db_ops = DatabaseOperations()
    db = DatabaseOperations()
    # a_log1 = AnalyseLogs()
    log = AnalyseLogs()
    db.db_connect()

    main()