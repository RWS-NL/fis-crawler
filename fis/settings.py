# Scrapy settings for vaarweginformatie project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from fis import __version__

BOT_NAME = "fis"

SPIDER_MODULES = ["fis.spiders"]
NEWSPIDER_MODULE = "fis.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = "fis (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)

CONCURRENT_REQUESTS = 32


# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    "fis.middlewares.VaarweginformatieSpiderMiddleware": 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    "fis.middlewares.VaarweginformatieDownloaderMiddleware": 543,
# }

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "fis.pipelines.PerGeoTypeExportPipeline": 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True

# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = "httpcache"
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
FIS_EXPORT_DIR = "output/fis-export"

# Dataset version used for output paths.
# Normalize to a "base" version for filesystem paths by stripping any
# local version segment (e.g. "+g<hash>" from setuptools_scm).
_raw_version_for_paths = __version__ if __version__ != "unknown" else "0.0.0"
VERSION = _raw_version_for_paths.split("+", 1)[0]

# --- Graph Integration & Splicing Parameters ---

# Coordinate Reference System for Projected/Metric Operations
# Used for all metric calculations (distance, area, splicing).
# TODO: Replace with dynamic estimation (e.g., estimate_utm_crs) for non-NL support.
PROJECTED_CRS = "EPSG:28992"

# --- Splicing Buffers (Meters) ---
# These determine the "footprint" of a structure on a fairway section.
# The splicer "cuts out" this amount of the original fairway to insert the structure subgraph.

# Safety margin added to lock chamber lengths in 'detailed' mode.
# Total cut = (Max Chamber Length / 2) + DETAILED_LOCK_SPLICING_BUFFER_M
DETAILED_LOCK_SPLICING_BUFFER_M = 50.0

# Fixed cut distance for bridge structures.
BRIDGE_SPLICING_BUFFER_M = 10.0

# Nominal length (meters) assigned to bridge passage edges for simulation compatibility.
BRIDGE_PASSAGE_LENGTH_M = 2.0

# Minimal buffer for locks in 'simplified' mode.
# Prevents large lock complexes from "swallowing" nearby bridges or junctions.
SIMPLIFIED_LOCK_SPLICING_BUFFER_M = 10.0

# Upper bound for lock buffers in simplified mode.
SIMPLIFIED_LOCK_MAX_BUFFER_M = 50.0

# --- Spatial Matching Buffers ---
# Used to associate structures with other geographic features.

# Radius (meters) to associate a bridge with a nearby fairway section.
BRIDGE_SECTION_MATCH_BUFFER_M = 20.0

# Degree-based buffer for matching locks to fairway sections (EPSG:4326).
# 0.0001 degrees is approximately 10-11 meters in the Netherlands.
LOCK_SECTION_MATCH_BUFFER_DEG = 0.0001

# Radius (meters) to match FIS bridge records to DISK physical bridge geometries.
DISK_MATCH_BUFFER_BRIDGE_M = 200.0

# Radius (meters) to match FIS lock records to DISK physical lock geometries.
DISK_MATCH_BUFFER_LOCK_M = 50.0

# --- Structure Identification ---

# Max distance (meters) to consider a bridge opening part of a nearby lock complex.
EMBEDDED_STRUCTURE_MAX_DIST_M = 500.0

# --- Berth Association ---

# Max linear distance (meters) to associate a berth with a lock.
BERTH_MATCH_MAX_DIST_M = 2000.0

# Degree-based buffer to exclude berths that sit inside the internal lock geometry.
BERTH_INTERNAL_SECTION_BUFFER_DEG = 0.00005
