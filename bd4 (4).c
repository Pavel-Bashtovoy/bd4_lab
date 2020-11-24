#include <stdio.h>
#include <stdlib.h>
#include <sqlca.h>

//возвращает оператор и код ошибки
void error_f(const char* place, int code)
{
	printf("error %d at %s \n", code, place);
	printf("error: %s\n", sqlca.sqlerrm.sqlerrmc);
	return;
}

int Query1()
{
	printf("query 1:\n");
	EXEC SQL begin declare section;
	int count;
	EXEC SQL end declare section;
	//начинаем транзакции
	EXEC SQL begin work;
	//выполнение запроса
	EXEC SQL
        select count(distinct n_det) into :count
        from spj
        where n_izd in (select n_izd from spj
                        join p on spj.n_det = p.n_det
                        where p.ves * spj.kol between 5000 and 6000);       
	
	if (sqlca.sqlcode < 0)
	{
		error_f("query1", sqlca.sqlcode);
		EXEC SQL rollback work;
	}
	else if (sqlca.sqlcode == 0)
	{
		printf("number of parts: %d\n", count);	
		EXEC SQL commit work;
	}
	else
	{
		return 0;
	}
	
}

int Query2()
{
	printf("query 2:\n");
	EXEC SQL begin work;

	EXEC SQL
        update p set ves = (case when p.town = 'Рим'
            then (select p1.ves ves1
                  from p p1
                  where p1.town = 'Париж'
                  order by p1.ves
                  limit 1)
            else (select p2.ves ves2
                  from p p2
                  where p2.town = 'Рим'
                  order by p2.ves
                  limit 1)
            end) 
        where p.town = 'Рим'
        or
        p.town = 'Париж';

	if (sqlca.sqlcode < 0)
	{
		error_f("query 2", sqlca.sqlcode);
		EXEC SQL rollback work;
	}
	else if (sqlca.sqlcode == 0)
	{
		printf("sql query completed.\n");
		printf("%ld lines processed\n", sqlca.sqlerrd[2]);
		EXEC SQL commit work;
	}
	else
	{
		return 0;
	}	
}

int Query3()
{
	printf("query3:\n");
	// Объявление основных переменных
	EXEC SQL begin declare section;
	char n_det[12];
	int kol, mvol;
	EXEC SQL end declare section;
	// Определить курсор
	EXEC SQL declare cursor3 cursor for
		select distinct a.n_det, a.kol vol, b.mvol
		from spj a
		join(select t.n_det, max(t.kol) mvol
			from spj t
			join s on s.n_post = t.n_post
	where s.town = 'Париж'
		group by t.n_det
		) b on b.n_det = a.n_det
	where a.kol <= b.mvol / 2
		order by 1, 2;
	if (sqlca.sqlcode < 0) {
		error_f("declare cursor", sqlca.sqlcode);
		return -1;
	}
	else
	{
		EXEC SQL begin work;
		// Открыть курсор
		EXEC SQL open cursor3;
		if (sqlca.sqlcode < 0)
		{
			error_f("cursor open", sqlca.sqlcode);
			EXEC SQL rollback work;
			return -1;
		}

		EXEC SQL fetch next from cursor3 into : n_det, : kol, : mvol;
		if (sqlca.sqlcode < 0)
		{
			error_f("fetch", sqlca.sqlcode);
			EXEC SQL close cursor3;
			EXEC SQL rollback work;
			return -1;
		}
		if (sqlca.sqlcode == 100)
		{
			printf("no results\n");
			EXEC SQL close cursor3;
			return 0;
		}
		else
		{
			// Вывод результата
			printf("n_det\tvol\tmvol\n");

			do
			{
				if (sqlca.sqlcode == 0)
				{
					printf("%s\t%d\t%d\n", n_det, kol, mvol);
				}
				EXEC SQL fetch next from cursor3 into : n_det, : kol, : mvol;
				if (sqlca.sqlcode < 0)
				{
					error_f("fetch", sqlca.sqlcode);
					EXEC SQL rollback work;
					break;
				}
			} while (sqlca.sqlcode != 100);

			// Закрыть курсор
			EXEC SQL close cursor3;
			EXEC SQL commit work;
		}
		
	}
	return 0;
}



int Query4()
{
	printf("query 4:\n");
	EXEC SQL begin declare section;
	char n_post[12];
	EXEC SQL end declare section;
	EXEC SQL declare cursor4 cursor for

        select n_post 
        from s
        except
        select n_post 
        from spj
        where n_det in (select a.n_det
                        from spj a
                        join j on j.n_izd = a.n_izd
                        where j.town = 'Париж');    

	if (sqlca.sqlcode < 0)
		error_f("declare cursor", sqlca.sqlcode);
	else
	{
		EXEC SQL begin work;
		EXEC SQL open cursor4;
		if (sqlca.sqlcode < 0)
		{
			error_f("cursor open", sqlca.sqlcode);
			EXEC SQL rollback work;
		}
		EXEC SQL fetch next from cursor4 into :n_post;
		if (sqlca.sqlcode == 100)
		{
			printf("no results\n");
			EXEC SQL close cursor4;
			EXEC SQL commit work;
			return 0;
		}
		else if (sqlca.sqlcode < 0)
		{
			error_f("fetch", sqlca.sqlcode);
			EXEC SQL close cursor4;
			EXEC SQL rollback work;
		}
		printf("n_post\n");

		do
		{
			printf("%s\n", n_post);
			EXEC SQL fetch next from cursor4 into :n_post;
			if (sqlca.sqlcode < 0)
			{
				error_f("fetch", sqlca.sqlcode);
				EXEC SQL close cursor4;
				EXEC SQL rollback work;
			}
		} while (sqlca.sqlcode != 100);

		EXEC SQL close cursor4;
		EXEC SQL commit work;
	}
	return 0;
}

int Query5()
{
	printf("query5:\n");
	EXEC SQL begin declare section;
	char n_det[12], name[22],cvet[22],ves[12],town[21];
	EXEC SQL end declare section;
	EXEC SQL declare cursor5 cursor for

        SELECT p.*
        FROM spj t
        JOIN p ON p.n_det = t.n_det
        WHERE t.n_post IN (SELECT s.n_post
                           FROM s
                           WHERE s.town ='Афины')
        EXCEPT
        SELECT p.*
        FROM spj t
        JOIN p ON p.n_det = t.n_det
        WHERE NOT t.n_post IN (SELECT s.n_post
                               FROM s
                               WHERE s.town='Афины');


	if (sqlca.sqlcode != 0)
		error_f("declare cursor", sqlca.sqlcode);
	else
	{
		EXEC SQL begin work;
		EXEC SQL open cursor5;
		if (sqlca.sqlcode < 0)
		{
			error_f("cursor open", sqlca.sqlcode);
			EXEC SQL rollback work;
		}
		EXEC SQL fetch next from cursor5 into :n_det, :name, :cvet, :ves, :town;
		if (sqlca.sqlcode == 100)
		{
			printf("no results\n");
			EXEC SQL close cursor5;
			EXEC SQL commit work;
			return 0;
		}
		else if (sqlca.sqlcode < 0)
		{
			error_f("Fetch", sqlca.sqlcode);
			EXEC SQL close cursor5;
			EXEC SQL rollback work;

		}
		printf("n_det\tname\t\t\tcvet\t\tves\ttown\n");

		do
		{
			printf("%s\t%s\t%s\t%s\t%s\n", n_det, name,cvet,ves,town);
			EXEC SQL fetch next from cursor5 into :n_det, :name, :cvet, :ves, :town;
			if (sqlca.sqlcode < 0)
			{
				error_f("fetch", sqlca.sqlcode);
				EXEC SQL close cursor5;
				EXEC SQL rollback work;

			}
		} while (sqlca.sqlcode != 100);

		EXEC SQL close cursor5;
		EXEC SQL commit work;
	}
	return 0;
}

int main()
{
	//конектимся к базе данных students на сервере fpm2
	EXEC SQL connect to students@fpm2.ami.nstu.ru user "pmi-b7104" using "JiHanth6";
	
	//ошибка в запросе
	if (sqlca.sqlcode < 0)
	{
		error_f("connection to database", sqlca.sqlcode);
		return 1;
	}
	else printf("successful connected to database\n");

	//устанавливает путь к схеме
	EXEC SQL set search_path to "pmib7104";
	if (sqlca.sqlcode != 0)
	{
		error_f("set database", sqlca.sqlcode);
		return 1;
	}

	int k, flag = 0;
	do
	{
		printf("write a number from 1 to 5 to complete the request or 6 to exit:");
		scanf("%d", &k);
		switch (k)
		{
		case 1:
			Query1();
			break;
		case 2:
			Query2();
			break;
        case 3:
			Query3();
			break;
		case 4:
			Query4();
			break;
		case 5:
			Query5();
			break;
		case 6:
			flag = 1;
			printf("session ended\n");
			EXEC SQL DISCONNECT CURRENT;
			break;
		default:
			printf("use only number from 1 to 6!\n");
			break;
		}
	} while (flag == 0);
	return 0;
}
