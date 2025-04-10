import os
from dotenv import load_dotenv
from loguru import logger
from etl.bitget_etl import BitgetETL
from models.bitget_models import BitgetConfig

def load_config() -> BitgetConfig:
    """Load and validate configuration from environment variables."""
    load_dotenv()
    
    config = {
        'base_url': os.getenv('BITGET_BASE_URL'),
        'affiliates': []
    }
    
    # Load affiliate configurations
    affiliate_ids = os.getenv('BITGET_AFFILIATE_IDS', '').split(',')
    for aff_id in affiliate_ids:
        if not aff_id.strip():
            continue
            
        config['affiliates'].append({
            'id': aff_id.strip(),
            'name': os.getenv(f'BITGET_AFFILIATE_{aff_id}_NAME'),
            'api_key': os.getenv(f'BITGET_AFFILIATE_{aff_id}_API_KEY'),
            'api_secret': os.getenv(f'BITGET_AFFILIATE_{aff_id}_API_SECRET'),
            'api_passphrase': os.getenv(f'BITGET_AFFILIATE_{aff_id}_API_PASSPHRASE')
        })
    
    # Validate config using Pydantic model
    return BitgetConfig(**config)

def main():
    """Run the Bitget ETL pipeline for all affiliates."""
    try:
        # Load and validate configuration
        config = load_config()
        
        # Initialize ETL
        etl = BitgetETL(config.dict())
        
        # Run ETL for each affiliate
        for affiliate in config.affiliates:
            affiliate_id = affiliate.id
            logger.info(f"Processing affiliate: {affiliate_id}")
            
            # Run ETL for affiliate-level data
            etl.run_etl(affiliate_id)
            
            # Optionally, run ETL for specific clients
            # client_ids = os.getenv(f'BITGET_AFFILIATE_{affiliate_id}_CLIENT_IDS', '').split(',')
            # for client_id in client_ids:
            #     if client_id.strip():
            #         etl.run_etl(affiliate_id, client_id.strip())
                
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 