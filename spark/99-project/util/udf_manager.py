from urllib.parse import urlparse
from user_agents import parse

class UDFManager:
    @staticmethod
    def get_country(url: str):
        if not url:
            return None
        
        try:
            parsed_url = urlparse(url).netloc
            country = parsed_url.split(".")[-1]
            return country
        except Exception as e:
            return None

    @staticmethod
    def get_browser(ua: str):
        if not ua:
            return None
        try:
            user_agent = parse(ua)
            return user_agent.browser.family
        except Exception as e:
            return None
        
        
    @staticmethod
    def get_os(ua: str):
        if not ua:
            return None
        try:
            user_agent = parse(ua)
            return user_agent.os.family
        except Exception as e:
            return None