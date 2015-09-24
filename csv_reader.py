import csv

with open('bms-sensor-data-types.csv', 'rU') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='|')

    limit = 10
    current = 0

    for row in reader:
    	# Some rows in the csv are wrongfully parsed
        if current > limit:
            break

        if (len(row)) > 5:
            print(row[1], row[5])
        current += 1