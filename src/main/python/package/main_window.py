import pandas as pd
from PySide2 import QtWidgets, QtCore

from package.api.v2.Scraper import Scraper
from package.api.constants import *

from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg


class Worker(QtCore.QObject):
    finished = QtCore.Signal()

    def __init__(self, scraper, path):
        super().__init__()
        self.scraper = scraper
        self.path = path

    def run(self):
        self.scraper.scrape_data(self.path)
        self.finished.emit()


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.scraper = Scraper()
        self.setup_ui()

    def setup_ui(self):
        self.create_widgets()
        self.create_layouts()
        self.modify_widgets()
        self.add_widgets_to_layouts()
        self.add_actions_to_toolbar()
        self.setup_connections()

    def create_widgets(self):
        self.toolbar = QtWidgets.QToolBar()
        self.tree_widget = QtWidgets.QTreeWidget()
        self.main_widget = QtWidgets.QWidget()
        self.indexes_canvas = FigureCanvasQTAgg(Figure(figsize=(4, 3)))
        self.ax_indexes = self.indexes_canvas.figure.add_subplot(111)
        self.corr_canvas = FigureCanvasQTAgg(Figure(figsize=(4, 3)))
        self.ax_corr = self.corr_canvas.figure.add_subplot(111)

    def modify_widgets(self):
        self.tree_widget.setColumnCount(len(INDEXES))
        self.tree_widget.setHeaderLabels(INDEXES)

    def create_layouts(self):
        self.main_layout = QtWidgets.QGridLayout(self.main_widget)

    def add_widgets_to_layouts(self):
        self.addToolBar(QtCore.Qt.TopToolBarArea, self.toolbar)
        self.setCentralWidget(self.main_widget)
        self.main_layout.addWidget(self.tree_widget, 0, 0, 2, 2)
        self.main_layout.addWidget(self.indexes_canvas, 2, 0, 1, 1)
        self.main_layout.addWidget(self.corr_canvas, 2, 1, 1, 1)

    def add_actions_to_toolbar(self):
        self.act_scrape = self.toolbar.addAction("Scrape data")
        self.act_load = self.toolbar.addAction("Load data")
        self.act_save = self.toolbar.addAction("Save Data")

    def setup_connections(self):
        self.act_scrape.triggered.connect(self.scrape)
        self.act_load.triggered.connect(self.open)
        self.act_save.triggered.connect(self.save)
        self.tree_widget.itemClicked.connect(self.update_indexes_diag)

    def open(self):
        file_dialog = QtWidgets.QFileDialog(self)
        file_dialog.setMimeTypeFilters(["application/json"])
        if file_dialog.exec_() == QtWidgets.QDialog.Accepted:
            file = file_dialog.selectedUrls()[0].path()[1:]
            self.scraper.load_data(file)
            self.load_tree_widget()
            self.update_corr_diag()

    def scrape(self):
        file_dialog = QtWidgets.QFileDialog(self)
        file_dialog.setMimeTypeFilters(["text/csv"])
        if file_dialog.exec_() == QtWidgets.QDialog.Accepted:
            file = file_dialog.selectedUrls()[0].path()[1:]
            self.thread = QtCore.QThread(self)
            self.worker = Worker(scraper=self.scraper, path=file)
            self.worker.moveToThread(self.thread)
            self.thread.started.connect(self.worker.run)
            self.worker.finished.connect(self.thread.quit)
            self.worker.finished.connect(self.worker.deleteLater)
            self.thread.finished.connect(self.thread.deleteLater)
            self.thread.start()
            self.act_scrape.setEnabled(False)
            self.act_load.setEnabled(False)
            self.act_save.setEnabled(False)
            self.thread.finished.connect(lambda: (
                self.load_tree_widget(),
                self.update_corr_diag(),
                self.act_scrape.setEnabled(True),
                self.act_load.setEnabled(True),
                self.act_save.setEnabled(True)
            ))

    def save(self):
        file_dialog = QtWidgets.QFileDialog(self)
        file_dialog.setAcceptMode(QtWidgets.QFileDialog.AcceptSave)
        file_dialog.setMimeTypeFilters(["application/json"])
        if file_dialog.exec_() == QtWidgets.QDialog.Accepted:
            file = file_dialog.selectedUrls()[0].path()[1:]
            self.scraper.save_data(file)

    def load_tree_widget(self):
        self.tree_widget.clear()
        for country in self.scraper.countries:
            parent_tree_item = QtWidgets.QTreeWidgetItem()
            for i, v in enumerate(country.array()):
                parent_tree_item.setText(i, str(v))
            for city in country.cities:
                child_tree_item = QtWidgets.QTreeWidgetItem()
                for i, v in enumerate(city.array()):
                    child_tree_item.setText(i, str(v))
                parent_tree_item.addChild(child_tree_item)
            self.tree_widget.addTopLevelItem(parent_tree_item)

    def update_corr_diag(self):
        df = pd.DataFrame.from_dict(self.scraper.csv)
        corr = df.corr()[["Fatality", "Incidence"]].iloc[2:]
        print(corr)
        self.ax_corr.clear()
        self.ax_corr.matshow(corr)
        self.ax_corr.set_yticks(range(0, len(INDEXES)-3))
        self.ax_corr.set_yticklabels(INDEXES[3:])
        self.corr_canvas.draw()

    def update_indexes_diag(self, item, column):
        self.ax_indexes.clear()
        y = ["Quality of life", "Health Care", "Traffic", "Pollution", "Climate"]
        x = [float(item.text(3)), float(item.text(4)), float(item.text(13)), float(item.text(16)), float(item.text(17))]
        self.ax_indexes.barh(y, x)
        self.ax_indexes.invert_yaxis()
        self.ax_indexes.set_title(f"{item.text(0)}")
        self.indexes_canvas.draw()
