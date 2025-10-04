import inspect
import psycopg2
import psycopg2.extras
import logging
from time import sleep as wait


class DB:
    def __init__(self, connection_link, logger=None, extensions_scheme='extensions'):
        """
        Initialize the database handler class.

        :param connection_link: PostgreSQL connection string
        :param logger: optional logger instance (if not provided, default logger will be created)
        :param extensions_scheme: schema name for extensions (default: 'extensions')
        """

        self.conn_link = connection_link
        if not logger:
            # Create a default logger if one is not provided
            formatter = logging.Formatter(fmt=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger = logging.getLogger('db')
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)
            self.logger = logger
        else:
            self.logger = logger

        if not self.conn_link:
            # Fatal error: missing connection string
            self.logger.critical(f'missing database connection link')
            exit()

        self.extensions_scheme = extensions_scheme

        # Retry policy for DB connections
        self.max_connection_retries = 10
        self.connection_retry_timeout = 1

    def get_connection(self):
        """
        Attempt to establish a database connection with retry logic.
        Retries up to `max_connection_retries` times with a delay.

        :return: psycopg2 connection object OR False if all attempts failed
        """

        for _ in range(self.max_connection_retries):
            try:
                conn = psycopg2.connect(self.conn_link)
                return conn
            except Exception as e:
                self.logger.error(f'connection exception::{e}')
                wait(self.connection_retry_timeout)
                continue

        self.logger.error(f'maximum connection attempts has been reached')
        return False

    def get_method_args(self, method, search='connection'):
        """
        Check if a method has a specific argument (e.g., 'cursor', 'connection').
        Used by decorators to decide whether to pass DB objects.

        :param method: method to inspect
        :param search: argument name to search for
        :return: True if argument found, else False
        """

        try:
            args = inspect.getfullargspec(method).args
            if search in args:
                return True
            return False
        except:
            return False

    def cursor(method):
        """
        Decorator to handle database connection and cursor management.
        - Opens a connection if needed
        - Provides a cursor (RealDictCursor) to the wrapped function
        - Closes connection and cursor after use
        """

        def wrapper(self, *args, **kwargs):
            conn = None
            cur = None

            try:
                # Check if method expects a cursor argument
                if self.get_method_args(method, 'cursor'):
                    conn = self.get_connection()
                    if not conn:
                        self.logger.error(f'failed to establish database connection')
                        return False

                    # Create a RealDictCursor (results returned as dicts instead of tuples)
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                    kwargs.update({'cursor': cursor})

                    # Pass connection as well if the method requires it
                    if self.get_method_args(method, 'connection'):
                        kwargs.update({'connection': conn})

                else:
                    self.logger.info('connection kw not found in decorated method')

                return method(self, *args, **kwargs)

            except Exception as e:
                self.logger.exception(f'unexpected exception occurred during connection establishing :: {e}')
            finally:
                # Ensure cleanup
                conn.close() if conn else None
                cur.close() if cur else None
        return wrapper

    def prepare_statement(self, update_keys, conflict_key, **optional_parameters):
        """
       Build an UPSERT (ON CONFLICT) SQL statement.

       :param update_keys: list of column names to update
       :param conflict_key: column(s) used as conflict keys
       :param optional_parameters: extra column=value pairs for update
       :return: SQL ON CONFLICT clause as a string OR False on error
       """

        try:
            # Build "col=excluded.col" for each update key
            up = [f'{k}=excluded.{k}' for k in update_keys]

            # Add optional custom update expressions
            if optional_parameters:
                opt = [f'{k}={v}' for k, v in optional_parameters.items()]
                opt = ', '.join(opt) if opt else None
                up.append(opt)

            up = ', '.join(up)

            # Add optional custom update expressions
            conflict_key = conflict_key if isinstance(conflict_key, str) else ','.join(conflict_key)
            return f"ON CONFLICT({conflict_key}) DO UPDATE SET {up}"
        except Exception as e:
            self.logger.exception(f'failed to prepare statement:: {update_keys} :: {e}')
            return False

    @cursor
    def save_batch(self, result, table_name=None, cursor=None, connection=None, on_conflict_stmt=None):
        """
        Insert a batch of rows (list of dicts) into a table.

        :param result: list of dictionaries OR a single dictionary
        :param table_name: name of the target table
        :param cursor: provided by @cursor decorator
        :param connection: provided by @cursor decorator
        :param on_conflict_stmt: optional ON CONFLICT statement
        :return: True if insert succeeded, False otherwise
        """

        if not table_name:
            self.logger.error(f'table name is required parameter')
            return False

        if not hasattr(result, '__iter__'):
            self.logger.error(f'wrong saving data type:: {type(result)}')
            return False

        if len(result) == 0:
            self.logger.warning(f'nothing to save: list is empty')
            return True

        # save single dictionary item
        if isinstance(result, dict):
            longest_column_keys = list(result.keys())
            result = [result]
        # extract keys from the longest dict item to determine columns structure
        else:
            longest_column_keys = list(max(result, key=len))

        signs, mog, args_str, insert_statement, columns = [None] * 5

        try:
            signs = '(' + ('%s,' * len(longest_column_keys))[:-1] + ')'
            mog = list()
            for x in result:
                # fill NULL's for non existed columns during multiple dictionary items saving
                values = [x.get(key, None) for key in longest_column_keys]
                r = cursor.mogrify(signs, tuple(values))
                mog.append(r)
                del values, r

            # combine values part
            args_str = b','.join(mog).decode()

            # prepare columns part
            columns = ', '.join(longest_column_keys)

            insert_statement = f"""INSERT INTO {table_name}(""" + columns + """) VALUES """
            if on_conflict_stmt:
                cursor.execute(insert_statement + args_str + " " + on_conflict_stmt.strip())
            else:
                cursor.execute(insert_statement + args_str)

            connection.commit()

            self.logger.info(f'data saved:: {table_name}')

            return True

        except Exception as e:
            self.logger.exception(f'exception occurred while saving::{e}')
            return False
        finally:
            del signs, mog, args_str, insert_statement, columns, longest_column_keys

    def save_products(self, result, on_conflict_stmt=None):
        """
        Convenience wrapper to save products into bigbasket.products table.
        """

        return self.save_batch(result, table_name='bigbasket.products', on_conflict_stmt=on_conflict_stmt)

    @cursor
    def get_results(self, cursor=None):
        """
        Fetch all products from bigbasket.products table.

        :param cursor: provided by @cursor decorator
        :return: list of dictionaries OR False on error
        """

        try:
            query = f"""
                SELECT product_id,
                name,
                brand,
                product_url,
                images,
                unit,
                quantity_label,
                price_mrp,
                price_sp,
                discount_percent,
                is_best_value,
                available_quantity,
                availability_code,
                category_main,
                category_mid,
                category_leaf,
                created_at_on_web_site::text,
                updated_at_on_web_site::text
            FROM bigbasket.products;
            """

            cursor.execute(query)
            results = cursor.fetchall()

            return [dict(item) for item in results]
        except Exception as e:
            self.logger.error("Unexpected error on getting result data")
            return False