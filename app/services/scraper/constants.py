# -*- coding: utf-8 -*-
"""
Constants and mappings for the AdaptiveScraper
"""

# Base URL for the website
BASE_URL = "http://vitibrasil.cnpuv.embrapa.br/index.php"

# Category mappings (URL parameters)
CATEGORY_MAPPING = {
    "producao": "opt_02",
    "processamento": "opt_03",
    "comercializacao": "opt_04",
    "importacao": "opt_05",
    "exportacao": "opt_06",
}

# Subcategory mappings for each category
SUBCATEGORY_MAPPING = {
    "processamento": {
        "viniferas": "subopt_01",
        "americanas": "subopt_02",
        "mesa": "subopt_03",
        "semclassificacao": "subopt_04"
    },
    "importacao": {
        "vinhos": "subopt_01",
        "espumantes": "subopt_02",
        "sucos": "subopt_05",
        "passas": "subopt_06",
        "frescas": "subopt_07"
    },
    "exportacao": {
        "vinhos": "subopt_01",
        "espumantes": "subopt_02",
        "sucos": "subopt_04",
        "uvas": "subopt_05"
    },
    "producao": {
        "uva": "subopt_01",
        # IMPORTANTE: Na fonte de dados, vinho_mesa e vinho_fino compartilham o mesmo parâmetro URL "subopt_02".
        # A distinção entre eles é feita internamente pelo sistema durante o processamento, baseado nas 
        # características da uva (viníferas vs. americanas) e nomenclatura do produto.
        # Ver ProducaoProcessor._classify_product() para a lógica de classificação detalhada.
        "vinho_mesa": "subopt_02",  # Vinho de mesa comum (americanas/híbridas)
        "vinho_fino": "subopt_02",  # Vinho fino (vinífera) - mesmo subopt que vinho_mesa
        "suco": "subopt_03",
        "derivados": "subopt_04"
    }
}

# Alternative names mapping for subcategories (handles case and format variations)
SUBCATEGORY_ALIASES = {
    "producao": {
        "UVA": "uva",
        "VINHO DE MESA": "vinho_mesa",
        "VINHO_MESA": "vinho_mesa",
        "VINHO_DE_MESA": "vinho_mesa", 
        "VINHODEMESA": "vinho_mesa",
        "VINHO": "vinho_mesa",
        "VINHO FINO": "vinho_fino",
        "VINHO FINO DE MESA": "vinho_fino",
        "VINHO FINO DE MESA (VINIFERA)": "vinho_fino",
        "VINIFERA": "vinho_fino",
        "VINÍFERAS": "vinho_fino",
        "VINÍFERA": "vinho_fino",
        "SUCO": "suco",
        "DERIVADOS": "derivados"
    },
    "processamento": {
        "VINÍFERAS": "viniferas",
        "VINIFERAS": "viniferas",
        "AMERICANAS": "americanas",
        "MESA": "mesa",
        "SEM CLASSIFICAÇÃO": "semclassificacao",
        "SEM CLASSIFICACAO": "semclassificacao"
    },
    "importacao": {
        "VINHOS": "vinhos",
        "ESPUMANTES": "espumantes",
        "SUCOS": "sucos",
        "PASSAS": "passas",
        "FRESCAS": "frescas",
        "UVAS FRESCAS": "frescas"
    },
    "exportacao": {
        "VINHOS": "vinhos",
        "ESPUMANTES": "espumantes",
        "SUCOS": "sucos",
        "UVAS": "uvas"
    }
}

# Display names for subcategories (for UI presentation)
SUBCATEGORY_DISPLAY_NAMES = {
    "producao": {
        "uva": "Uva",
        "vinho_mesa": "Vinho de Mesa",
        "vinho_fino": "Vinho Fino (Vinífera)",
        "suco": "Suco",
        "derivados": "Derivados"
    },
    "processamento": {
        "viniferas": "Viníferas",
        "americanas": "Americanas",
        "mesa": "Mesa",
        "semclassificacao": "Sem Classificação"
    },
    "importacao": {
        "vinhos": "Vinhos",
        "espumantes": "Espumantes",
        "sucos": "Sucos",
        "passas": "Passas",
        "frescas": "Uvas Frescas"
    },
    "exportacao": {
        "vinhos": "Vinhos",
        "espumantes": "Espumantes",
        "sucos": "Sucos",
        "uvas": "Uvas"
    }
}

# Default cultivar data for classifier
DEFAULT_CULTIVAR_DATA = {
    "viniferas": [
        "Alicante Bouschet", "Ancelota", "Aramon", "Alfrocheiro", "Ancellotta",
        "Barbera", "Bonarda", "Cabernet Franc", "Cabernet Sauvignon", "Caladoc",
        "Carmenère", "Castelão", "Corvina", "Dornfelder", "Gamay Noir", 
        "Kanthus", "Magliocco", "Malbec", "Marselan", "Merlot", "Moscato Bailey", 
        "Moscato Preto", "Mourvèdre", "Muscat Noir", "Nebbiolo", "Petit Verdot", 
        "Pinot Meunier", "Pinot Noir", "Pinotage", "Primitivo", "Rebo", 
        "Ruby Cabernet", "Sangiovese", "Syrah", "Tannat", "Tempranillo", 
        "Teroldego", "Touriga Franca", "Touriga Nacional", "Trebbiano", "Trincadeira",
        "Viognier", "Cabernet", "Sauvignon", "Moscato", "Chardonnay",
        "Riesling", "Sauvignon Blanc", "Gewürztraminer", "Semillon", "Chenin Blanc"
    ],
    "americanas": [
        "Isabel", "Concord", "Bordô", "Bordó", "Niagara", "Niágara", "Jacquez", "Herbemont", 
        "Seibel", "BRS Magna", "BRS Violeta", "BRS Rúbea", "BRS Cora",
        "Seyve Villard", "Martha", "Cunningham", "Goethe"
    ],
    "mesa": [
        "Italia", "Itália", "Rubi", "Benitaka", "Red Globe", "Niagara Rosada", 
        "Crimson", "Thompson", "Perlette", "BRS Vitória", 
        "BRS Isis", "BRS Nubia", "BRS Morena", "BRS Clara", "BRS Linda"
    ]
}