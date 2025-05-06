import asyncpg
import os
import aiohttp
import logging
import asyncio
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

async def create_tables(pool):
    async with pool.acquire() as conn:
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
                reported_by_trusted BOOLEAN
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
        logger.info("Database tables created or already exist")

async def fetch_reports():
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
            "chains": ["ETH"],
            "scamCategories": [],
            "orderBy": {
                "field": "UPVOTES_COUNT",
                "direction": "DESC"
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
        pool = await asyncpg.create_pool(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )
        
        # Создаем таблицы, если они не существуют
        await create_tables(pool)
        
        logger.info("Starting to fetch reports from ChainAbuse API")
        processed_reports = 0
        processed_addresses = 0
        
        async with pool.acquire() as connection:
            # Решаем, нужно ли очищать таблицы перед началом работы
            clear_tables = os.getenv('CLEAR_EXISTING_DATA', 'false').lower() == 'true'
            if clear_tables:
                async with connection.transaction():
                    await connection.execute('DELETE FROM report_addresses')
                    await connection.execute('DELETE FROM reports')
                    logger.info("Cleared existing data from the database")

            has_next_page = True
            cursor = None
            
            while has_next_page:
                if cursor:
                    variables["after"] = cursor
                    payload["variables"] = variables
                
                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.post(url, json=payload, headers=headers) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                logger.error(f"API returned error {response.status}: {error_text}")
                                break
                            
                            data = await response.json()
                    except Exception as e:
                        logger.error(f"Error during API request: {str(e)}")
                        break
                
                if 'errors' in data:
                    logger.error(f"GraphQL errors: {data['errors']}")
                    break
                
                if not data.get('data') or not data['data'].get('reports') or not data['data']['reports'].get('edges'):
                    logger.warning("No report data received from API")
                    break
                
                reports_data = data['data']['reports']
                reports = reports_data['edges']
                
                if not reports:
                    logger.info("No more reports to process")
                    break
                
                page_info = reports_data.get('pageInfo', {})
                has_next_page = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')
                
                logger.info(f"Processing {len(reports)} reports")
                
                for report in reports:
                    try:
                        node = report['node']
                        
                        # Получаем информацию о пользователе, создавшем отчет
                        reported_by = node.get('reportedBy', {}) or {}
                        
                        # Проверяем, является ли пользователь доверенным
                        is_trusted = reported_by.get('trusted', False)
                        
                        # Если пользователь не доверенный, пропускаем отчет
                        if not is_trusted:
                            continue
                        
                        # Проверка на существование отчета
                        exists = await connection.fetchval(
                            'SELECT 1 FROM reports WHERE id = $1', node['id']
                        )
                        
                        if exists:
                            continue
                        
                        # Используем подготовленный запрос с явным указанием типов
                        await connection.execute('''
                            INSERT INTO reports(id, is_private, created_at, scam_category, category_description, 
                                bi_directional_vote_count, viewer_did_vote, description, comments_count, 
                                source, checked, reported_by_id, reported_by_username, reported_by_trusted)
                            VALUES($1, $2, $3::TEXT, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
                            is_trusted  # Используем переменную is_trusted вместо повторного доступа к словарю
                        )
                        
                        processed_reports += 1
                        
                        # Сохраняем связанные адреса только для доверенных отчетов
                        addresses = node.get('addresses', []) or []
                        for address in addresses:
                            if not address:
                                continue
                                
                            await connection.execute('''
                                INSERT INTO report_addresses(id, report_id, address, chain)
                                VALUES($1, $2, $3, $4)
                                ON CONFLICT (id) DO NOTHING
                            ''', 
                                address['id'], 
                                node['id'], 
                                address.get('address', ''), 
                                address.get('chain', '')
                            )
                            processed_addresses += 1
                    except Exception as e:
                        logger.error(f"Error processing report: {str(e)}")
                        # Продолжаем обработку других отчетов
                
                logger.info(f"Processed page with {len(reports)} reports. Has next page: {has_next_page}")
        
        logger.info(f"Finished fetching reports. Processed {processed_reports} reports and {processed_addresses} addresses")
        
        await pool.close()
        return {
            "status": "success",
            "processed_reports": processed_reports,
            "processed_addresses": processed_addresses
        }
        
    except Exception as e:
        logger.exception(f"Error fetching reports: {str(e)}")
        raise

async def main():
    logger.info("ChainAbuse parser started")
    
    try:
        result = await fetch_reports()
        logger.info(f"Parser completed successfully: {result}")
    except Exception as e:
        logger.error(f"Parser failed with error: {str(e)}")
        exit(1)
    
    logger.info("ChainAbuse parser finished")

if __name__ == "__main__":
    asyncio.run(main())