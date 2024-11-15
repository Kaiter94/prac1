#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <mutex>

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;

struct Column {
    string name;
    Column* next = nullptr;
    Column* prev = nullptr;
};

struct Row {
    string data;
    Row* next = nullptr;
    Row* prev = nullptr;
};

struct Table {
    string name;
    Column* columns = nullptr;
    Row* rows = nullptr;
    int pk_sequence = 1;
    int rowCount = 0;
    Table* next = nullptr;
    Table* prev = nullptr;
    mutex lock;
};

struct Database {
    string name;
    int tuples_limit;
    Table* tables = nullptr;
    void loadSchema(const string& schemaPath);
    void createFileStructure();
    void insertRow(const string& tableName, const string& values);
    void deleteRows(const string& tableName, const string& condition);
    void selectRows(const string& tableName, const string& condition);
    void crossJoinSelect(const string& tableName1, const string& tableName2, const string& columns);
    void parseAndExecuteCommand(const string& command);
};

struct Condition {
    string column;
    string value;
    string operation;
};

const int MAX_CONDITIONS = 10;
void addTable(Database* db, const string& tableName) {
    Table* newTable = new Table();
    newTable->name = tableName;

    if (!db->tables) {
        db->tables = newTable;
    } else {
        Table* temp = db->tables;
        while (temp->next) temp = temp->next;
        temp->next = newTable;
        newTable->prev = temp;
    }
}

void addColumn(Table* table, const string& columnName) {
    Column* newColumn = new Column();
    newColumn->name = columnName;
    if (!table->columns) {
        table->columns = newColumn;
    } else {
        Column* temp = table->columns;
        while (temp->next) temp = temp->next;
        temp->next = newColumn;
        newColumn->prev = temp;
    }
}

void Database::loadSchema(const string& schemaPath) {
    ifstream schemaFile(schemaPath);
    if (!schemaFile) {
        cerr << "Не удалось открыть файл конфигурации" << endl;
        return;
    }
    json schemaJson;
    schemaFile >> schemaJson;
    this->name = schemaJson["name"];
    this->tuples_limit = schemaJson["tuples_limit"];
    for (auto& [tableName, columns] : schemaJson["structure"].items()) {
        addTable(this, tableName);
        Table* currentTable = tables;
        while (currentTable->next) currentTable = currentTable->next;
        for (const auto& columnName : columns) {
            addColumn(currentTable, columnName);
        }
    }
}

void Database::createFileStructure() {
    fs::create_directory(this->name);
    Table* table = this->tables;
    while (table) {
        string tableDir = this->name + "/" + table->name;
        fs::create_directory(tableDir);
        string dataFilePath = tableDir + "/1.csv";
        if (!fs::exists(dataFilePath)) {
            ofstream dataFile(dataFilePath);
            Column* col = table->columns;
            while (col) {
                dataFile << col->name << (col->next ? "," : "\n");
                col = col->next;
            }
            dataFile.close();
        }

        ifstream dataFile(dataFilePath);
        string line;
        bool isHeader = true;
        while (getline(dataFile, line)) {
            if (isHeader) {
                isHeader = false;
                continue;
            }
            Row* newRow = new Row();
            newRow->data = line;

            if (!table->rows) {
                table->rows = newRow;
            } else {
                Row* temp = table->rows;
                while (temp->next) temp = temp->next;
                temp->next = newRow;
                newRow->prev = temp;
            }
            table->rowCount++;
        }
        dataFile.close();

        table = table->next;
    }
}

int parseConditions(const string& condition, Condition conditions[]) {
    size_t pos = 0;
    int count = 0;
    string currentOp = "AND";
    
    while (pos < condition.length() && count < MAX_CONDITIONS) {
        size_t nextAnd = condition.find(" AND ", pos);
        size_t nextOr = condition.find(" OR ", pos);
        size_t end = min(nextAnd == string::npos ? condition.length() : nextAnd,
                         nextOr == string::npos ? condition.length() : nextOr);
        string token = condition.substr(pos, end - pos);
        size_t eqPos = token.find('=');
        if (eqPos != string::npos) {
            conditions[count].column = token.substr(0, eqPos);
            conditions[count].value = token.substr(eqPos + 1);
            conditions[count].column.erase(0, conditions[count].column.find_first_not_of(" "));
            conditions[count].column.erase(conditions[count].column.find_last_not_of(" ") + 1);
            conditions[count].value.erase(0, conditions[count].value.find_first_not_of(" '"));
            conditions[count].value.erase(conditions[count].value.find_last_not_of(" '") + 1);
            conditions[count].operation = currentOp;
            count++;
        }
        if (end == nextAnd) {
            currentOp = "AND";
            pos = end + 5;
        } else if (end == nextOr) {
            currentOp = "OR";
            pos = end + 4;
        } else {
            pos = end;
        }
    }
    return count;
}

void Database::insertRow(const string& tableName, const string& values) {
    Table* table = this->tables;
    while (table && table->name != tableName) {
        table = table->next;
    }
    if (table) {
        lock_guard<mutex> guard(table->lock);
        if (table->rowCount < this->tuples_limit) {
            Row* newRow = new Row();
            string cleanedValues = values;
            cleanedValues.erase(0, cleanedValues.find_first_not_of(" ('"));
            cleanedValues.erase(cleanedValues.find_last_not_of(" ')") + 1);
            stringstream ss(cleanedValues);
            string value;
            string rowData;
            while (getline(ss, value, ',')) {
                value.erase(0, value.find_first_not_of(" '"));
                value.erase(value.find_last_not_of(" '") + 1);
                rowData += value + ",";
            }
            if (!rowData.empty()) {
                rowData.pop_back();
            }
            newRow->data = rowData;
            if (!table->rows) {
                table->rows = newRow;
            } else {
                Row* temp = table->rows;
                while (temp->next) temp = temp->next;
                temp->next = newRow;
                newRow->prev = temp;
            }
            table->rowCount++;
            table->pk_sequence++;
            string tableDir = this->name + "/" + table->name;
            ofstream dataFile(tableDir + "/1.csv", ios_base::app);
            dataFile << rowData << "\n";
            dataFile.close();
        } else {
            cerr << "Превышен лимит строк для таблицы: " << tableName << endl;
        }
    } else {
        cerr << "Таблица не найдена: " << tableName << endl;
    }
}

void Database::selectRows(const string& tableName, const string& condition) {
    Table* table = this->tables;
    while (table && table->name != tableName) {
        table = table->next;
    }
    if (!table) {
        cerr << "Таблица не найдена: " << tableName << endl;
        return;
    }
    Condition conditions[MAX_CONDITIONS];
    int conditionCount = parseConditions(condition, conditions);
    Row* row = table->rows;
    while (row) {
        bool match = (conditionCount > 0) ? (conditions[0].operation == "OR" ? false : true) : true;
        for (int i = 0; i < conditionCount; i++) {
            bool conditionMet = (row->data.find(conditions[i].value) != string::npos);
            if (conditions[i].operation == "AND") {
                match = match && conditionMet;
            } else if (conditions[i].operation == "OR") {
                match = match || conditionMet;
            }
        }
        if (match) {
            cout << row->data << endl;
        }
        row = row->next;
    }
}

void Database::deleteRows(const string& tableName, const string& condition) {
    Table* table = this->tables;
    while (table && table->name != tableName) {
        table = table->next;
    }
    if (!table) {
        cerr << "Таблица не найдена: " << tableName << endl;
        return;
    }
    Condition conditions[MAX_CONDITIONS];
    int conditionCount = parseConditions(condition, conditions);
    Row* row = table->rows;
    while (row) {
        bool match = (conditionCount > 0) ? (conditions[0].operation == "OR" ? false : true) : true;
        for (int i = 0; i < conditionCount; i++) {
            bool conditionMet = (row->data.find(conditions[i].value) != string::npos);
            if (conditions[i].operation == "AND") {
                match = match && conditionMet;
            } else if (conditions[i].operation == "OR") {
                match = match || conditionMet;
            }
        }
        if (match) {
            if (row->prev) row->prev->next = row->next;
            if (row->next) row->next->prev = row->prev;
            if (row == table->rows) table->rows = row->next;
            Row* temp = row;
            row = row->next;
            delete temp;
            table->rowCount--;
        } else {
            row = row->next;
        }
    }
    string tableDir = this->name + "/" + table->name + "/1.csv";
    ofstream dataFile(tableDir, ios::trunc);
    Column* col = table->columns;
    while (col) {
        dataFile << col->name << (col->next ? "," : "\n");
        col = col->next;
    }
    row = table->rows;
    while (row) {
        dataFile << row->data << "\n";
        row = row->next;
    }
    dataFile.close();
}

void Database::crossJoinSelect(const string& tableName1, const string& tableName2, const string& columns) {
    Table* table1 = this->tables;
    Table* table2 = this->tables;
    while (table1 && table1->name != tableName1) {
        table1 = table1->next;
    }
    while (table2 && table2->name != tableName2) {
        table2 = table2->next;
    }
    if (table1 && table2) {
        Row* row1 = table1->rows;
        while (row1) {
            Row* row2 = table2->rows;
            while (row2) {
                cout << row1->data << " | " << row2->data << endl;
                row2 = row2->next;
            }
            row1 = row1->next;
        }
    } else {
        if (!table1) cerr << "Таблица не найдена: " << tableName1 << endl;
        if (!table2) cerr << "Таблица не найдена: " << tableName2 << endl;
    }
}

void Database::parseAndExecuteCommand(const string& command) {
    string action;
    istringstream stream(command);
    stream >> action;
    if (action == "SELECT") {
        size_t fromPos = command.find(" FROM ");
        if (fromPos == string::npos) {
            cerr << "Ошибка в синтаксисе: Ожидается ключевое слово 'FROM'" << endl;
            return;
        }
        string columns = command.substr(7, fromPos - 7);
        string afterFrom = command.substr(fromPos + 6);
        size_t wherePos = afterFrom.find(" WHERE ");
        string tableName;
        string condition;
        if (wherePos != string::npos) {
            tableName = afterFrom.substr(0, wherePos);
            condition = afterFrom.substr(wherePos + 7);
        } else {
            tableName = afterFrom;
        }
        tableName.erase(0, tableName.find_first_not_of(" "));
        tableName.erase(tableName.find_last_not_of(" ") + 1);
        if (tableName.find(",") != string::npos) {
            string tableName1 = tableName.substr(0, tableName.find(","));
            string tableName2 = tableName.substr(tableName.find(",") + 1);
            tableName1.erase(0, tableName1.find_first_not_of(" "));
            tableName1.erase(tableName1.find_last_not_of(" ") + 1);
            tableName2.erase(0, tableName2.find_first_not_of(" "));
            tableName2.erase(tableName2.find_last_not_of(" ") + 1);
            crossJoinSelect(tableName1, tableName2, columns);
        } else {
            selectRows(tableName, condition);
        }
    } else if (action == "DELETE") {
        string from, tableName, condition;
        stream >> from >> tableName;
        string whereKeyword;
        if (stream >> whereKeyword && whereKeyword == "WHERE") {
            getline(stream, condition);
        }
        deleteRows(tableName, condition);
    } else if (action == "INSERT") {
        string into, tableName, values;
        stream >> into >> tableName >> action;
        getline(stream, values, ')');
        insertRow(tableName, values.substr(1));
    } else {
        cerr << "Неизвестная команда: " << action << endl;
    }
}

int main() {
    Database db;
    db.loadSchema("schema.json");
    db.createFileStructure();
    string command;
    cout << "Введите SQL-команду (или 'exit' для выхода):" << endl;
    while (true) {
        cout << "> ";
        getline(cin, command);
        if (command == "exit") {
            break;
        }
        db.parseAndExecuteCommand(command);
    }
    return 0;
}

