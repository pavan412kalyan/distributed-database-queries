import psycopg2


conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=admin")
cur = conn.cursor()
#cur.execute("""
#    CREATE TABLE uusers(
#    id integer PRIMARY KEY,
#    email text,
#    name text,
#    address text
#)
#""")



cur.execute("INSERT INTO users VALUES (%s, %s, %s, %s)", (14, 'hello@dataquest.io', 'Some Name', '123 Fake St.'))


cur.close()
conn.commit()