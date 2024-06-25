import codecs
import logging
import re
from typing import Optional

from charset_normalizer import from_bytes


class XMLMetadataParser:
    """XMLやHTMLのメタデータを解析するためのクラス"""
    DESCRIPTION_TAGS = {
        'dc.description', 'dc:description',
        'dcterms.abstract', 'dcterms.description',
        'description', 'sailthru.description', 'twitter:description'
    }
    TITLE_TAGS = {
        'citation_title', 'dc.title', 'dcterms.title', 'fb_title',
        'headline', 'parsely-title', 'sailthru.title', 'shareaholic:title',
        'rbtitle', 'title', 'twitter:title'
    }
    UNICODE_ALIASES = {'utf-8', 'utf_8'}

    def __init__(self):
        # codecs.register(self.custom_codec_search)
        self.lang_pattern = re.compile(b'<html.*lang="(.*?)"')
        self.encoding_pattern = re.compile(
            b'(?:'
            b'<\\?xml.*?encoding=["\']([\w-]+)["\']|'
            b'<meta[^>]+charset=["\']([\w-]+)["\']|'
            b'<meta[^>]+charset=([\w-]+)(?:\s|>)|'
            b'<meta\s+http-equiv=["\'](?:Content-Type|content-type)["\'][^>]+content=["\'].*?charset=([\w-]+)["\']'
            b')',
            re.IGNORECASE | re.DOTALL
        )

        description_tags_pattern = b'|'.join(tag.encode('ascii') for tag in self.DESCRIPTION_TAGS)
        self.description_pattern = re.compile(
            b'<meta[^>]+(?:name|property)=["\'](' + description_tags_pattern + b')["\'][^>]+content=["\'](.*?)["\']'
        )

        title_tags_pattern = b'|'.join(tag.encode('ascii') for tag in self.TITLE_TAGS)
        self.title_pattern = re.compile(
            b'<meta[^>]+(?:name|property)=["\'](' + title_tags_pattern + b')["\'][^>]+content=["\'](.*?)["\']'
        )
        self.html_title_pattern = re.compile(b'<title>(.*?)</title>')

        self.heading_pattern = re.compile(b'<h[1-6][^>]*>(.*?)</h[1-6]>', re.IGNORECASE | re.DOTALL)

    def parse_lang(self, xml_data: bytes) -> Optional[str]:
        """XMLデータから言語を解析する"""
        lang = self.lang_pattern.search(xml_data)
        return lang.group(1).decode('ascii') if lang else None

    def parse_encoding(self, xml_data: bytes) -> Optional[str]:
        """XMLデータからエンコーディングを解析する"""
        match = self.encoding_pattern.search(xml_data)
        if match:
            # グループのうち、最初に見つかった非Noneの値を返す
            encoding = next((group for group in match.groups() if group is not None), None)
            return encoding.decode('ascii', errors='ignore') if encoding else None
        return None

    def parse_description(self, xml_data: bytes) -> Optional[str]:
        """XMLデータからdescriptionの内容を解析する"""
        matches = self.description_pattern.findall(xml_data)
        if matches:
            # 最も長い内容を持つマッチを選択
            longest_match = max(matches, key=lambda x: len(x[1]))
            return self.decode_data(longest_match[1])
        return None

    def parse_title(self, xml_data: bytes) -> Optional[str]:
        """XMLデータからタイトルを解析する"""
        # メタタグからタイトルを探す
        matches = self.title_pattern.findall(xml_data)
        if matches:
            # 最も長い内容を持つマッチを選択
            longest_match = max(matches, key=lambda x: len(x[1]))
            return self.decode_data(longest_match[1])

        # メタタグにタイトルがない場合、<title>タグを探す
        match = self.html_title_pattern.search(xml_data)
        if match:
            return self.decode_data(match.group(1))

        return None

    def parse_heading(self, xml_data: bytes) -> Optional[str]:
        """XMLデータから最も長い見出し（h1-h6）の内容を解析する"""
        matches = self.heading_pattern.findall(xml_data)
        if matches:
            # HTMLタグを除去し、最も長い見出しを選択
            cleaned_headings = [re.sub(b'<[^>]+>', b'', heading).strip() for heading in matches]
            longest_heading = max(cleaned_headings, key=len)
            return self.decode_data(longest_heading)
        return None

    def isutf8(self, data):
        """Simple heuristic to determine if a bytestring uses standard unicode encoding"""
        try:
            data.decode('UTF-8')
        except UnicodeDecodeError:
            return False
        return True

    def detect_encoding(self, bytesobject):
        """"Read all input or first chunk and return a list of encodings"""
        # alternatives: https://github.com/scrapy/w3lib/blob/master/w3lib/encoding.py
        # unicode-test
        if self.isutf8(bytesobject):
            return ['utf-8']
        guesses = []
        # try charset_normalizer on first part, fallback on full document
        if len(bytesobject) < 10000:
            detection_results = from_bytes(bytesobject)
        else:
            detection_results = from_bytes(bytesobject[:5000] + bytesobject[-5000:]) or \
                                from_bytes(bytesobject)
        # return alternatives
        if len(detection_results) > 0:
            guesses.extend([r.encoding for r in detection_results])
        # it cannot be utf-8 (tested above)
        return [g for g in guesses if g not in self.UNICODE_ALIASES]

    def decode_data(self, filecontent):
        """Check if the bytestring could be GZip and eventually decompress it,
           guess bytestring encoding and try to decode to Unicode string.
           Resort to destructive conversion otherwise."""
        # init
        if isinstance(filecontent, str):
            return filecontent
        htmltext = None
        # encoding
        for guessed_encoding in self.detect_encoding(filecontent):
            try:
                htmltext = filecontent.decode(guessed_encoding)
            except (LookupError, UnicodeDecodeError):  # VISCII: lookup
                logging.warning('wrong encoding detected: %s', guessed_encoding)
                htmltext = None
            else:
                break
        # return original content if nothing else succeeded
        return htmltext or str(filecontent, encoding='utf-8', errors='replace')