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
        previous_value = self.model.data(index, PandasTableModel.PreviousValueRole)
        if previous_value is "" or current_value is "":
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

class PandasTableModel(QAbstractTableModel):
    PreviousValueRole = Qt.ItemDataRole.UserRole + 1

    def __init__(self, data:pandas.DataFrame, parent=None):
        super(PandasTableModel, self).__init__(parent=parent)
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