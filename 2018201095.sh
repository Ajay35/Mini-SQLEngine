python main.py "select max(A) from table1"
python main.py "select max(A) from table1" | wc -l


python main.py "select min(B) from table2"
python main.py "select min(B) from table2" | wc -l

python main.py "select avg(C) from table1"
python main.py "select avg(C) from table1" | wc -l

python main.py "select sum(D) from table2"
python main.py "select sum(D) from table2" | wc -l

python main.py "select table1.A,table2.D from table1,table2"
python main.py "select table1.A,table2.D from table1,table2" | wc -l

python main.py "select distinct(C) from table1"
python main.py "select distinct(C) from table1" | wc -l

python main.py "select table1.B,table1.C from table1 where A=-900"
python main.py "select table1.B,table1.C from table1 where A=-900" | wc -l

python main.py "select table1.A,table1.B from table1 where A=775 OR B=803"
python main.py "select table1.A,table1.B from table1 where A=775 OR B=803" | wc -l


python main.py "select * from table1,table2"
python main.py "select * from table1,table2" | wc -l

python main.py "select * from table1,table2 where table1.B=table2.B"
python main.py "select * from table1,table2 where table1.B=table2.B" | wc -l

python main.py "select table1.A,table2.D from table1,table2 where table1.B=table2.B"
python main.py "select table1.A,table2.D from table1,table2 where table1.B=table2.B" | wc -l

python main.py "select table1.C from table1,table2 where table1.A<table2.B"
python main.py "select table1.C from table1,table2 where table1.A<table2.B" | wc -l

python main.py "select A from table4"
python main.py "select A from table4" | wc -l

python main.py "select Z from table1"
python main.py "select Z from table1" | wc -l

python main.py "select table1.B from table1,table2"
python main.py "select table1.B from table1,table2" | wc -l

python main.py "select distinct(A,B) from table1"
python main.py "select distinct(A,B) from table1" | wc -l

python main.py "select table1.C from table1,table2 where table1.A<table2.D OR table1.A>table2.B"
python main.py "select table1.C from table1,table2 where table1.A<table2.D OR table1.A>table2.B" | wc -l

python main.py "select table1.C from table1,table2 where table1.A=table2.D"
python main.py "select table1.C from table1,table2 where table1.A=table2.D" | wc -l

python main.py "select table1.A from table1,table2 where table1.A<table2.B AND table1.A>table2.D"
python main.py "select table1.A from table1,table2 where table1.A<table2.B AND table1.A>table2.D" | wc -l

python main.py "select sum(A) from table1,table2"
python main.py "select sum(A) from table1,table2" | wc -l
