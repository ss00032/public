SELECT TABLE_NAME
     , COLUMN_NAME
  FROM ALL_TAB_COLUMNS

list = /home/tstream/project.dat

i = 1

grep $list -e | while read -r line
do
  sqlquery = $(echo $line | cut -f $i ';')
  echo -e $sqlquery > "sqlquery$i.txt"
  i += 1
  
done