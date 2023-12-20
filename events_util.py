from collections import namedtuple

from psycopg2 import connect, extras

from parse_module.connection.database import ParsingDB
from parse_module.manager.core import BotCore
from parse_module.utils import utils

write = BotCore()

def make_tickets(event_id, scheme_id):
    '''
    Создать билеты у event_id
    взять схему для билетов в public.tables_constructor с scheme_id
    '''
    db_main = ParsingDB()
    db_main.connect_db()
    scheme_box = db_main.get_scheme(scheme_id)
    scheme_json = scheme_box[-1]

    Ticket = namedtuple('Ticket', [
        'x_coord', 'y_coord', 'sector', 'sector_id',
        'row', 'seat', 'status', 'original_price', 'sell_price',
        'event_id', 'scheme_id', 'no_schema_available'])
    
    all_tickets = []

    for ticket in scheme_json["seats"]:
        sector = scheme_json["sectors"][ticket[3]]
        if "count" in sector:
            new_dance_ticket = {
                    'x_coord':0,
                    'y_coord':0,
                    'sector':sector["name"],
                    'sector_id':ticket[3],
                    'row':"no_schema",
                    'seat':"no_schema",
                    'status':"not",
                    'original_price':1,
                    'sell_price':1,
                    'event_id':event_id,
                    'scheme_id':scheme_id,
                    'no_schema_available':0
            }
            tickets = Ticket(**new_dance_ticket)
            all_tickets.append(tuple(tickets))
        else:
            new_ticket = {
                "x_coord":ticket[0],
                "y_coord":ticket[1],
                "sector":sector["name"],
                "sector_id":ticket[3],
                "row":ticket[5],
                "seat":ticket[6],
                "status": str('not'),
                "original_price":1,
                "sell_price":1,
                "event_id":event_id,
                "scheme_id":scheme_id,
                'no_schema_available':0
            }
            tickets = Ticket(**new_ticket)
            all_tickets.append(tuple(tickets))
    print(len(all_tickets))
    insert_query = '''
        INSERT INTO public.tables_tickets (
            x_coord, y_coord, sector, sector_id, 
            row, seat, status, original_price, sell_price, 
            event_id_id, scheme_id_id,  no_schema_available
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    try:
        db_main.cursor.execute('BEGIN;')

        chunk_size = 1000

        for i in range(0, len(all_tickets), chunk_size):
            chunk = all_tickets[i:i + chunk_size]
            db_main.cursor.executemany(insert_query, chunk)
        db_main.cursor.executemany(insert_query, chunk)
        write.bprint(f'write success {event_id}, {scheme_id}',color=utils.Fore.GREEN)
    except Exception as ex:
        db_main.cursor.execute('ROLLBACK;')
        print(f'Error occurred {ex}')
    finally:
        db_main.commit()


def make_tickets_new(event_id, scheme_id):
    '''
    Создать билеты у event_id
    взять схему для билетов в public.tables_constructor с scheme_id
    '''
    db_main = ParsingDB()
    db_main.connect_db()
    scheme_box = db_main.get_scheme(scheme_id)
    scheme_json = scheme_box[-1]

    Ticket = namedtuple('Ticket', [
        'x_coord', 'y_coord', 'sector', 'sector_id',
        'row', 'seat', 'status', 'original_price', 'sell_price',
        'event_id', 'scheme_id', 'no_schema_available'])
    
    all_tickets = []

    for ticket in scheme_json["seats"]:
        sector = scheme_json["sectors"][ticket[3]]
        if "count" in sector:
            new_dance_ticket = {
                    'x_coord':0,
                    'y_coord':0,
                    'sector':sector["name"],
                    'sector_id':ticket[3],
                    'row':"no_schema",
                    'seat':"no_schema",
                    'status':"not",
                    'original_price':1,
                    'sell_price':1,
                    'event_id':event_id,
                    'scheme_id':scheme_id,
                    'no_schema_available':0
            }
            tickets = Ticket(**new_dance_ticket)
            all_tickets.append(tuple(tickets))
        else:
            new_ticket = {
                "x_coord":ticket[0],
                "y_coord":ticket[1],
                "sector":sector["name"],
                "sector_id":ticket[3],
                "row":ticket[5],
                "seat":ticket[6],
                "status": str('not'),
                "original_price":1,
                "sell_price":1,
                "event_id":event_id,
                "scheme_id":scheme_id,
                'no_schema_available':0
            }
            tickets = Ticket(**new_ticket)
            all_tickets.append(tuple(tickets))
    print(len(all_tickets))
    insert_query = '''
        INSERT INTO public.tables_tickets (
            x_coord, y_coord, sector, sector_id, 
            row, seat, status, original_price, sell_price, 
            event_id_id, scheme_id_id,  no_schema_available
        ) VALUES %s
    '''
    try:
        extras.execute_values(db_main.cursor, insert_query, all_tickets, template=None, page_size=100)
        write.bprint(f'write success {event_id}, {scheme_id}',color=utils.Fore.GREEN)
    except Exception as ex:
        print(f'Error occurred {ex}')
    finally:
        db_main.commit()

if __name__ == '__main__':
    from parse_module.utils.provision import multi_try
    event_id = 23540
    scheme_id = 60
    multi_try(make_tickets_new, name='Make tickets', tries=5, kwargs={'event_id':event_id,
                                                              'scheme_id':scheme_id})


