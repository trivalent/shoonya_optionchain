import pandas
from PySide6.QtCore import Qt, QAbstractTableModel
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QStyledItemDelegate


class QHighlightDelegate(QStyledItemDelegate):
    def __init__(self, model):
        super().__init__()
        self.model = model

    def paint(self, painter, option, index):
        super().paint(painter, option, index)
        current_value = self.model.data(index, Qt.ItemDataRole.DisplayRole)
        previous_value = self.model.data(index, OptionChainTableModel.PreviousValueRole)
        if previous_value == "" or current_value == "":
            option.backgroundBrush = QColor(Qt.GlobalColor.yellow)
        elif float(current_value) < float(previous_value):
            option.backgroundBrush = QColor(Qt.GlobalColor.red)
        elif float(current_value) > float(previous_value):
            option.backgroundBrush = QColor(Qt.GlobalColor.green)
        painter.save()
        painter.fillRect(option.rect, option.backgroundBrush)
        QStyledItemDelegate.paint(self, painter, option, index)
        painter.restore()

    def flags(self, index):
        return Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled

class OptionChainTableModel(QAbstractTableModel):
    PreviousValueRole = Qt.ItemDataRole.UserRole + 1

    def __init__(self, data:pandas.DataFrame, parent=None):
        super(OptionChainTableModel, self).__init__(parent=parent)
        self._data = data
        self.columns = ["CALL Price", "Strike", "PUT Price"]
        self._previous_values = {}

    def rowCount(self, parent = ...):
        return len(self._data.values)

    def columnCount(self, parent = ...):
        return 3

    def data(self, index, role = Qt.ItemDataRole.DisplayRole):
        if index.isValid():
            if role == Qt.ItemDataRole.DisplayRole:
                return str(self._data.values[index.row()][index.column()])
            elif role == self.PreviousValueRole:
                return self._previous_values.get((index.row(), index.column()), "")
        return None

    def headerData(self, section, orientation, role = ...):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self.columns[section]

    def update_price(self, price_field, column, row, price):
        index = self.createIndex(row, column)
        key = (index.row(), index.column())
        self._previous_values[key] = str(self._data.loc[row, price_field])
        self._data.loc[row, price_field] = float(price)
        self.dataChanged.emit(index, index)

    def flags(self, index):
        return Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled if index.column() != 1 else ~Qt.ItemFlag.ItemIsSelectable


class PositionsTableModel(QAbstractTableModel):
    def __init__(self, data: pandas.DataFrame, parent=None):
        super(PositionsTableModel, self).__init__(parent=parent)
        self._data = data
        self.columns = ['Name', 'Option', 'Lots', 'Qty', 'Avg Price', 'LTP', 'P/L', 'Return %']
        self._previous_values = {}

    def rowCount(self, parent=...):
        return len(self._data.values)

    def columnCount(self, parent=...):
        return len(self.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if index.isValid():
            if role == Qt.ItemDataRole.DisplayRole:
                return str(self._data.values[index.row()][index.column()])
            # elif role == self.PreviousValueRole:
            #     return self._previous_values.get((index.row(), index.column()), "")
        return None

    def headerData(self, section, orientation, role=...):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self.columns[section]

    def update_price(self, price_field, column, row, price):
        index = self.createIndex(row, column)
        key = (index.row(), index.column())
        self._previous_values[key] = str(self._data.loc[row, price_field])
        self._data.loc[row, price_field] = float(price)
        # update P/L
        self._data.loc[row, price_field + 1] = (price - self._data.loc[row, price_field - 1]) * self._data.loc[row, 3]
        self._data.loc[row, price_field + 2] = 100 * (self._data.loc[row, price_field + 1] / (self._data.loc[row, 3] * self._data.loc[row, price_field - 1]))
        self.dataChanged.emit(self.createIndex(row, 0), self.createIndex(row, self.columnCount() - 1))