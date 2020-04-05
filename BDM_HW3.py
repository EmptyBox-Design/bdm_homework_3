from pyspark import SparkContext

def main(sc, file_path, output_folder):

  import csv
  
  def extractProducts(partId, records):

    if partId==0:
        next(records)
    
    reader = csv.reader(records)
    
    for row in reader:
      (product_ID,year,company) = (row[1], row[0].split("-")[0], row[7])
      yield ((product_ID.lower(),year,company.lower()), 1)

  def toCSVLine(data):
    return ','.join(str(d) for d in data)

  # def list_to_csv_str(x):
  #   output = io.StringIO("")
  #   csv.writer(output).writerow(x)
  #   return output.getvalue().strip() # remove extra newline

  product_data = sc.textFile(file_path, use_unicode=True).cache()

  products= product_data.mapPartitionsWithIndex(extractProducts)

  products.reduceByKey(lambda x,y: x+y)\
    .map(lambda x: ((x[0][0], x[0][1]), (x[1])))\
    .groupByKey()\
    .mapValues(lambda x: (sum(list(x)),len(x), max([int(round(((z / sum(list(x))) *100), 0)) for z in list(x)])))\
    .sortByKey() \
    .map(lambda x: list(x[0] + x[1]))\
    .map(toCSVLine)\
    .saveAsTextFile(output_folder)

if __name__ == "__main__":
  sc = SparkContext()
  # Execute the main function
  import sys

  file_location = sys.argv[1]
  output_folder = sys.argv[2]

  main(sc, file_location, output_folder)