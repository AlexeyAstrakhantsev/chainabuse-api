import asyncpg
import os
import aiohttp
import logging
import asyncio
import json
from typing import List, Dict, Any
from datetime import datetime, timezone

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"data/{os.getenv('LOG_FILE', 'parser.log')}"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Режим работы парсера
PARSE_MODE = os.getenv('PARSE_MODE', 'FULL')  # Возможные значения: FULL, NEW_ONLY

# Список сетей для парсинга
CHAINS = [
    "BTC", "BINANCE", "ETH", "SOL", "TRON", "POLYGON", "LITECOIN", 
    "ARBITRUM", "AVALANCHE", "HBAR", "BASE", "CARDANO", "MULTIVERSX", 
    "TON", "ALGORAND"
]

# Настройки сортировки из переменных окружения
ORDER_BY_FIELD = os.getenv('ORDER_BY_FIELD', 'UPVOTES_COUNT')  # По умолчанию сортировка по голосам
ORDER_BY_DIRECTION = os.getenv('ORDER_BY_DIRECTION', 'DESC')   # По умолчанию по убыванию

# В начале файла
MAX_CONSECUTIVE_EXISTING = int(os.getenv('MAX_CONSECUTIVE_EXISTING', '5'))

async def create_tables(pool):
    async with pool.acquire() as conn:
        # Проверяем, нужно ли пересоздать таблицы
        should_recreate = os.getenv('RECREATE_TABLES', 'false').lower() == 'true'
        
        if should_recreate:
            logger.info("Recreating database tables...")
            # Удаляем существующие таблицы в правильном порядке (из-за внешних ключей)
            await conn.execute('DROP TABLE IF EXISTS report_addresses')
            await conn.execute('DROP TABLE IF EXISTS reports')
            await conn.execute('DROP TABLE IF EXISTS unified_addresses')
            await conn.execute('DROP SEQUENCE IF EXISTS unified_addresses_id_seq')
        
        # Создаем последовательность для id в unified_addresses
        await conn.execute('''
            CREATE SEQUENCE IF NOT EXISTS unified_addresses_id_seq 
            INCREMENT 1 
            START 1 
            MINVALUE 1 
            MAXVALUE 2147483647 
            CACHE 1
        ''')
        
        # Создаем таблицу unified_addresses с увеличенными размерами полей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS unified_addresses (
                address character varying(100) NOT NULL,
                type character varying(50) NOT NULL,
                address_name character varying(100),
                labels json,
                source character varying(150),
                created_at timestamp without time zone NOT NULL DEFAULT timezone('utc'::text, now()),
                id integer NOT NULL DEFAULT nextval('unified_addresses_id_seq'::regclass),
                PRIMARY KEY (id)
            )
        ''')
        
        # Создаем таблицы с актуальной структурой
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                id TEXT PRIMARY KEY,
                is_private BOOLEAN,
                created_at TEXT,
                scam_category TEXT,
                category_description TEXT,
                bi_directional_vote_count INTEGER,
                viewer_did_vote BOOLEAN,
                description TEXT,
                comments_count INTEGER,
                source TEXT,
                checked BOOLEAN,
                reported_by_id TEXT,
                reported_by_username TEXT,
                reported_by_trusted BOOLEAN,
                chain TEXT
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS report_addresses (
                id TEXT PRIMARY KEY,
                report_id TEXT,
                address TEXT,
                chain TEXT,
                FOREIGN KEY (report_id) REFERENCES reports(id)
            )
        ''')
        
        await conn.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS unique_address ON unified_addresses(address);
        ''')
        
        logger.info("Database tables created or already exist")

async def fetch_reports_for_chain(chain, pool, clear_tables=False, start_cursor=None):
    # Добавляем переменную start_time в начало функции
    start_time = datetime.now()
    
    url = 'https://www.chainabuse.com/api/graphql-proxy'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {os.getenv("CHAINABUSE_API_TOKEN")}'
    }
    
    query = """
    query GetReports($input: ReportsInput, $after: String, $before: String, $last: Float, $first: Float) {
      reports(input: $input, after: $after, before: $before, last: $last, first: $first) {
        pageInfo {
          hasNextPage
          hasPreviousPage
          startCursor
          endCursor
          __typename
        }
        edges {
          node {
            id
            isPrivate
            createdAt
            scamCategory
            categoryDescription
            biDirectionalVoteCount
            viewerDidVote
            description
            commentsCount
            source
            checked
            reportedBy {
              id
              username
              trusted
            }
            accusedScammers {
              id
              info {
                id
                contact
                type
              }
            }
            addresses {
              id
              address
              chain
              domain
              label
            }
          }
        }
        count
        totalCount
      }
    }
    """
    
    variables = {
        "input": {
            "chains": [chain],
            "scamCategories": [],
            "orderBy": {
                "field": ORDER_BY_FIELD,
                "direction": ORDER_BY_DIRECTION
            }
        },
        "first": 100
    }
    
    payload = {
        "operationName": "GetReports",
        "variables": variables,
        "query": query
    }

    try:
        logger.info(f"Starting to fetch reports for chain {chain}")
        processed_reports = 0
        processed_addresses = 0
        skipped_reports = 0
        skipped_existing = 0  # Счетчик последовательных существующих отчетов
        
        # Путь к файлу для сохранения прогресса
        progress_file = f"data/progress_{chain}.json"
        
        # Проверяем, существует ли файл с прогрессом
        if os.path.exists(progress_file) and start_cursor is None:
            try:
                with open(progress_file, 'r') as f:
                    progress_data = json.load(f)
                    cursor = progress_data.get('cursor')
                    logger.info(f"Resuming from saved cursor for chain {chain}")
            except Exception as e:
                logger.warning(f"Error loading progress file for chain {chain}: {str(e)}")
                cursor = start_cursor
        else:
            cursor = start_cursor
            
        async with pool.acquire() as connection:
            # Только очищаем данные, если это первая цепочка и флаг clear_tables установлен
            if clear_tables:
                async with connection.transaction():
                    await connection.execute('DELETE FROM report_addresses')
                    await connection.execute('DELETE FROM reports')
                    logger.info("Cleared existing data from the database")

            has_next_page = True
            page_count = 0
            retry_count = 0
            max_retries = 5
            
            while has_next_page:
                page_count += 1
                if cursor:
                    variables["after"] = cursor
                    payload["variables"] = variables
                
                try:
                    # Добавляем задержку между запросами, чтобы не перегружать API
                    await asyncio.sleep(1)  # 1 секунда между запросами
                    
                    async with aiohttp.ClientSession() as session:
                        logger.debug(f"Requesting page {page_count} for chain {chain}")
                        async with session.post(url, json=payload, headers=headers) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                logger.error(f"API returned error {response.status} for chain {chain}: {error_text}")
                                
                                # Если получаем ошибку 429 или 5xx, делаем паузу и пробуем снова
                                if response.status in [429, 500, 502, 503, 504]:
                                    retry_count += 1
                                    if retry_count > max_retries:
                                        logger.warning(f"Max retries reached for chain {chain}, moving on")
                                        break
                                    
                                    # Экспоненциальная задержка: 2^retry_count секунд
                                    wait_time = 2 ** retry_count
                                    logger.info(f"Waiting {wait_time} seconds before retry {retry_count}/{max_retries} for chain {chain}")
                                    await asyncio.sleep(wait_time)
                                    continue
                                else:
                                    # Другие ошибки
                                    break
                            
                            data = await response.json()
                            # Сброс счетчика повторных попыток при успешном запросе
                            retry_count = 0
                except Exception as e:
                    logger.error(f"Error during API request for chain {chain}: {str(e)}")
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.warning(f"Max retries reached for chain {chain}, moving on")
                        break
                    
                    wait_time = 2 ** retry_count
                    logger.info(f"Waiting {wait_time} seconds before retry {retry_count}/{max_retries} for chain {chain}")
                    await asyncio.sleep(wait_time)
                    continue
                
                if 'errors' in data:
                    logger.error(f"GraphQL errors for chain {chain}: {data['errors']}")
                    break
                
                if not data.get('data') or not data['data'].get('reports') or not data['data']['reports'].get('edges'):
                    logger.warning(f"No report data received from API for chain {chain}")
                    break
                
                reports_data = data['data']['reports']
                reports = reports_data['edges']
                
                if not reports:
                    logger.info(f"No more reports to process for chain {chain}")
                    break
                
                page_info = reports_data.get('pageInfo', {})
                has_next_page = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')
                
                logger.info(f"Processing {len(reports)} reports for chain {chain} (page {page_count})")
                
                for report in reports:
                    try:
                        node = report['node']
                        
                        # Проверяем существование отчета в базе
                        exists = await connection.fetchval(
                            'SELECT 1 FROM reports WHERE id = $1', node['id']
                        )
                        
                        # В режиме NEW_ONLY, если отчет уже существует, увеличиваем счетчик
                        if PARSE_MODE == 'NEW_ONLY' and exists:
                            skipped_existing += 1
                            skipped_reports += 1
                            
                            # Если встретили несколько последовательных существующих отчетов,
                            # прекращаем обработку текущей сети
                            if skipped_existing >= MAX_CONSECUTIVE_EXISTING:
                                logger.info(f"Found {skipped_existing} consecutive existing reports for chain {chain}, stopping processing")
                                has_next_page = False
                                break
                            continue
                        else:
                            # Сбрасываем счетчик, если нашли новый отчет
                            skipped_existing = 0
                        
                        # Получаем информацию о пользователе, создавшем отчет
                        reported_by = node.get('reportedBy', {}) or {}
                        
                        # Проверяем, является ли пользователь доверенным
                        is_trusted = reported_by.get('trusted', False)
                        
                        # Если пользователь не доверенный, пропускаем отчет
                        if not is_trusted:
                            skipped_reports += 1
                            continue
                        
                        # Используем подготовленный запрос с явным указанием типов
                        await connection.execute('''
                            INSERT INTO reports(id, is_private, created_at, scam_category, category_description, 
                                bi_directional_vote_count, viewer_did_vote, description, comments_count, 
                                source, checked, reported_by_id, reported_by_username, reported_by_trusted,
                                chain)
                            VALUES($1, $2, $3::TEXT, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                        ''', 
                            node['id'], 
                            node.get('isPrivate', False), 
                            node.get('createdAt', ''),
                            node.get('scamCategory', ''), 
                            node.get('categoryDescription', ''),
                            node.get('biDirectionalVoteCount', 0), 
                            node.get('viewerDidVote'),
                            node.get('description', ''), 
                            node.get('commentsCount', 0),
                            node.get('source', ''), 
                            node.get('checked'),
                            reported_by.get('id', ''), 
                            reported_by.get('username', ''), 
                            is_trusted,
                            chain  # Добавляем информацию о сети
                        )
                        
                        processed_reports += 1
                        
                        # Сохраняем связанные адреса только для доверенных отчетов
                        addresses = node.get('addresses', []) or []
                        for address in addresses:
                            if not address:
                                continue
                                
                            # Получаем адрес    
                            addr = address.get('address', '')
                            addr_chain = address.get('chain', '')
                            
                            # Проверяем, что адрес не пустой
                            if not addr:  # Добавленная проверка на пустой адрес
                                continue  # Пропускаем пустые адреса
                                
                            # Сохраняем в таблицу report_addresses
                            await connection.execute('''
                                INSERT INTO report_addresses(id, report_id, address, chain)
                                VALUES($1, $2, $3, $4)
                                ON CONFLICT (id) DO NOTHING
                            ''', 
                                address['id'], 
                                node['id'], 
                                addr,  # Используем переменную addr вместо address.get('address', '')
                                addr_chain
                            )
                            processed_addresses += 1
                            
                            # Формируем данные для unified_addresses
                            if addr and addr_chain:
                                # Получаем имя автора отчета
                                reporter_username = reported_by.get('username', 'unknown')
                                
                                # Создаем source в формате "chainabuse marked by <автор отчета>"
                                source = f"chainabuse marked by {reporter_username}"
                                
                                # Сохраняем в таблицу unified_addresses
                                await connection.execute('''
                                    INSERT INTO unified_addresses(address, type, address_name, source)
                                    VALUES($1, $2, $3, $4)
                                    ON CONFLICT (address) DO NOTHING
                                ''', 
                                    addr, 
                                    'scam', 
                                    node.get('scamCategory', ''),
                                    source
                                )
                    except Exception as e:
                        logger.error(f"Error processing report for chain {chain}: {str(e)}")
                        continue
                
                logger.info(f"Processed page {page_count} with {len(reports)} reports for chain {chain}. Has next page: {has_next_page}")
                
                # После обработки страницы сохраняем прогресс
                if cursor:
                    try:
                        os.makedirs(os.path.dirname(progress_file), exist_ok=True)
                        with open(progress_file, 'w') as f:
                            json.dump({'cursor': cursor, 'page_count': page_count}, f)
                    except Exception as e:
                        logger.warning(f"Error saving progress for chain {chain}: {str(e)}")
                
                # Добавляем поддержку прерывания по времени
                # Если парсер работает более 2 часов, завершаем работу
                run_time = (datetime.now() - start_time).total_seconds() / 3600
                if run_time > 2:
                    logger.info(f"Parser has been running for {run_time:.2f} hours, stopping for chain {chain}")
                    break
            
            # Если мы вышли из цикла по отчетам из-за существующих отчетов,
            # прерываем обработку страниц
            if not has_next_page:
                break
        
        logger.info(f"Finished fetching reports for chain {chain}. "
                   f"Processed {processed_reports} trusted reports, "
                   f"skipped {skipped_reports} reports, "
                   f"saved {processed_addresses} addresses.")
        
        return {
            "status": "success",
            "chain": chain,
            "processed_reports": processed_reports,
            "processed_addresses": processed_addresses,
            "skipped_reports": skipped_reports,
            "pages_processed": page_count
        }
        
    except Exception as e:
        logger.exception(f"Error fetching reports for chain {chain}: {str(e)}")
        return {
            "status": "error",
            "chain": chain,
            "error": str(e)
        }

async def fetch_reports():
    try:
        pool = await asyncpg.create_pool(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )
        
        # Создаем таблицы, если они не существуют
        await create_tables(pool)
        
        logger.info(f"Starting to fetch reports for {len(CHAINS)} chains")
        
        total_stats = {
            "total_processed_reports": 0,
            "total_processed_addresses": 0,
            "total_skipped_reports": 0,
            "chains_processed": 0,
            "chains_failed": 0
        }
        
        # Если включен режим парсинга только новых данных, меняем порядок сортировки
        if PARSE_MODE == 'NEW_ONLY':
            # Для новых данных лучше сортировать по дате создания
            global ORDER_BY_FIELD, ORDER_BY_DIRECTION
            ORDER_BY_FIELD = 'CREATED_AT'
            ORDER_BY_DIRECTION = 'DESC'  # От новых к старым
            
            logger.info("Running in NEW_ONLY mode, fetching only new reports")
            
            # Можно ограничить количество обрабатываемых страниц для каждой цепи
            global MAX_PAGES_PER_CHAIN
            if not os.getenv('MAX_PAGES_PER_CHAIN'):
                MAX_PAGES_PER_CHAIN = 10  # По умолчанию 10 страниц в режиме NEW_ONLY
        
        # Очищаем таблицы только перед обработкой первой сети
        clear_tables = os.getenv('CLEAR_EXISTING_DATA', 'false').lower() == 'true'
        
        # Обрабатываем каждую сеть последовательно
        for chain in CHAINS:
            start_time = datetime.now()
            logger.info(f"Processing chain {chain} ({CHAINS.index(chain) + 1}/{len(CHAINS)})")
            
            result = await fetch_reports_for_chain(chain, pool, clear_tables=(clear_tables and CHAINS.index(chain) == 0))
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if result["status"] == "success":
                total_stats["total_processed_reports"] += result["processed_reports"]
                total_stats["total_processed_addresses"] += result["processed_addresses"]
                total_stats["total_skipped_reports"] += result["skipped_reports"]
                total_stats["chains_processed"] += 1
                
                logger.info(f"Chain {chain} processed in {duration:.2f} seconds")
            else:
                total_stats["chains_failed"] += 1
                logger.error(f"Chain {chain} processing failed in {duration:.2f} seconds: {result.get('error', 'Unknown error')}")
        
        # Закрываем соединение с базой данных
        await pool.close()
        
        logger.info(f"Finished fetching all reports. "
                   f"Total processed: {total_stats['total_processed_reports']} trusted reports, "
                   f"skipped: {total_stats['total_skipped_reports']} reports, "
                   f"saved: {total_stats['total_processed_addresses']} addresses. "
                   f"Chains processed successfully: {total_stats['chains_processed']}, "
                   f"failed: {total_stats['chains_failed']}.")
        
        return total_stats
        
    except Exception as e:
        logger.exception(f"Error in main fetch_reports function: {str(e)}")
        raise

async def main():
    start_time = datetime.now()
    logger.info("ChainAbuse parser started")
    
    try:
        result = await fetch_reports()
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Parser completed successfully in {total_duration:.2f} seconds: {result}")
    except Exception as e:
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        
        logger.error(f"Parser failed after {total_duration:.2f} seconds with error: {str(e)}")
        exit(1)
    
    logger.info("ChainAbuse parser finished")

if __name__ == "__main__":
    asyncio.run(main())