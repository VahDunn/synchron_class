from synchronizer.synchro_class import DatabaseSynchronizer




s_db_url = "postgresql://ilyas_apunov:pampampam@localhost:5432/postgres"
t_db_url = "postgresql://ilyas_apunov:pumpumpum@localhost:5434/postgres"


synchronizer = DatabaseSynchronizer(s_db_url, t_db_url)


tables_to_sync = ['users']
synchronizer.synchronize_database(tables_to_sync)
