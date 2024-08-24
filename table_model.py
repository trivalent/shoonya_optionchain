import pandas
from PyQt6 import QtCore
from PyQt6.QtCore import QAbstractTableModel, Qt


class PandasTableModel(QAbstractTableModel):
    def __init__(self, data:pandas.DataFrame, parent=None):
        super(PandasTableModel, self).__init__(parent=parent)
        self._data = data
        self.columns = ["CALL Price", "Strike", "PUT Price"]

    def rowCount(self, parent = ...):
        return len(self._data.values)

    def columnCount(self, parent = ...):
        return 3

    def data(self, index, role = Qt.ItemDataRole.DisplayRole):
        if index.isValid():
            if role == Qt.ItemDataRole.DisplayRole:
                return QtCore.QVariant(str(self._data.iloc[index.row()][index.column()]))
        return QtCore.QVariant()

    def headerData(self, section, orientation, role = ...):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self.columns[section]

    def update_price(self, price_field, column, row, price):
        index = self.createIndex(row, column)
        self._data[price_field][row] = price
        self.dataChanged.emit(index, index)
