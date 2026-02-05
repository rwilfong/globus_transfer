"""
Keyring Credential Manager 


A utility script to securely store Globus Confidential Client secrets using the system's native keyring

Setup:
    pip install keyring configparser

Usage (Interactive):
    python setup_keyring.py --interactive

Usage (Direct):
    python setup_keyring.pyy --client_id <UUID> --secret <SECRET>
"""

import keyring
import getpass
import argparse
import logging
import sys
import configparser

# Configuration
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def store_secret(service_id, client_id, secret):
    """
    Securely stores the secret in the keyring
    """
    try:
        keyring.set_password(service_id, client_id, secret)
        logger.info(f"Successfully stored secret for Client ID: {client_id}")
        
        # Immediate Verification
        test_val = keyring.get_password(service_id, client_id)
        if test_val == secret:
            print("Verification Successful: Secret is readable.")
        else:
            print("Verification Failed: Data mismatch in keyring.")
            
    except Exception as e:
        logger.error(f"Failed to store secret: {e}")
        sys.exit(1)

def main():
    # Load defaults from config if available
    config = configparser.ConfigParser()
    config.read('config_nightly.ini')
    
    default_service = config.get('keyring', 'service_name', fallback='Globus Archive')
    default_client = config.get('globus', 'client_id', fallback=None)

    parser = argparse.ArgumentParser(description="Securely store Globus secrets")
    parser.add_argument('--service', default=default_service, help="Keyring service name")
    parser.add_argument('--client_id', default=default_client, help="Globus Client UUID")
    parser.add_argument('--secret', help="Secret (Warning: will appear in shell history)")
    parser.add_argument('--interactive', action='store_true', help="Prompt for inputs")

    args = parser.parse_args()

    if args.interactive:
        print("Globus Keyring Setup")
        client_id = input(f"Enter Client ID [{args.client_id}]: ") or args.client_id
        secret = getpass.getpass("Enter Globus Secret: ")
        service_id = input(f"Enter Service Name [{args.service}]: ") or args.service
    else:
        client_id = args.client_id
        secret = args.secret
        service_id = args.service

    if not client_id or not secret:
        logger.error("Missing Client ID or Secret. Use --interactive or provide arguments.")
        sys.exit(1)

    store_secret(service_id, client_id, secret)

if __name__ == "__main__":
    main()