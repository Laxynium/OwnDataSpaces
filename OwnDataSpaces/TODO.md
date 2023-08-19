### Test list
 - [] There are two instances of OwnSpace\
and both are performing insert into same table\
then when getting all data from this table\
there should be only one record
 - [] There is a unique constraint on some field\
and two spaces are writing data with same field value\
then both operations should succeed
    - Here I should check constraints on single column
    - constrain spanning multiple columns
 - [] Test checking that table/column name matching some keyword does not cause errors