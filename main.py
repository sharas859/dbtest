# import CSVtoDB from ../src/CSVtoDB.py
from src import CSVtoDB
import os


def main():

    if not os.path.exists('./db'):
        os.makedirs('./db')
    if not os.path.exists('./data'):
        os.makedirs('./data')

    db = './db/measurements.db'
    csv = './data/'

    for file in os.listdir(csv):
        if file.endswith('.csv') or file.endswith('.tsv'):
            CSVtoDB.csv_to_db(db, csv + file)

    tests = CSVtoDB.get_test_names(db)
    for test in tests:
        print("\n" + str(CSVtoDB.get_metadata(db, test[0])) + "\n")
        CSVtoDB.print_dimensions(test[0], CSVtoDB.get_data(db, test[0]))

    unpickled0 = CSVtoDB.get_data(db, '20220907 Nachmessung Bauteil 5&6')
    unpickled1 = CSVtoDB.get_data(
        db, '20220907 Nachmessung Bauteil 5&6', 1.5, 2.5)
    unpickled2 = CSVtoDB.get_data(db, '20220907 Nachmessung Bauteil 5&6',
                                  1.5, 2.5, '2022-09-07 12:50:53.377918', '2022-09-07 12:51:01.181058')

    CSVtoDB.print_dimensions('no limits', unpickled0)
    CSVtoDB.print_dimensions('x limit', unpickled1)
    CSVtoDB.print_dimensions('x + t limit', unpickled2)


if __name__ == '__main__':
    main()
