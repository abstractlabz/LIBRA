def encrypt_password(password):
    """
    Encrypt a password using Fernet symmetric encryption.
    
    Args:
        password (str): The plain text password to encrypt
        
    Returns:
        str: The encrypted password (base64-encoded) or None if encryption fails
    """
    try:
        # Get encryption key from environment variable
        encryption_key = os.getenv("PASSWORD_ENCRYPTION_KEY")
        if not encryption_key:
            logging.error("Password encryption key not found in environment variables")
            return None
            
        # Ensure the key is properly formatted for Fernet (32 url-safe base64-encoded bytes)
        if len(encryption_key) != 44 or not encryption_key.endswith('='):
            # Generate a URL-safe base64-encoded 32-byte key from the provided key
            key_bytes = encryption_key.encode('utf-8')
            encryption_key = base64.urlsafe_b64encode(key_bytes.ljust(32)[:32]).decode('utf-8')
        
        # Create a Fernet cipher with the key
        cipher = Fernet(encryption_key.encode('utf-8'))
        
        # Encrypt the password
        encrypted_password = cipher.encrypt(password.encode('utf-8')).decode('utf-8')
        return encrypted_password
        
    except Exception as e:
        logging.exception("Password encryption failed: %s", e)
        return None