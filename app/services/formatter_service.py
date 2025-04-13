import csv
import io
import json
import pandas as pd
from typing import Dict, List, Any, Optional, Union


class FormatterService:
    """
    Serviço responsável por formatar os dados de saída em diferentes formatos
    """
    
    @staticmethod
    def format_output(data: List[Dict[str, Any]], format_type: str = "json", 
                     include_metadata: bool = False, metadata: Optional[Dict[str, Any]] = None) -> Union[str, bytes]:
        """
        Formata os dados de saída no formato solicitado
        
        Args:
            data: Dados a serem formatados
            format_type: Formato desejado (json, csv, parquet)
            include_metadata: Se deve incluir metadados (apenas para JSON)
            metadata: Dicionário de metadados adicionais
            
        Returns:
            Dados formatados no formato solicitado
        """
        if not data:
            if format_type == "json":
                if include_metadata:
                    return json.dumps({
                        "data": [],
                        "count": 0,
                        "metadata": metadata or {}
                    })
                return json.dumps([])
            elif format_type == "csv":
                return ""
            elif format_type == "parquet":
                df = pd.DataFrame()
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer)
                return parquet_buffer.getvalue()
        
        if format_type == "json":
            return FormatterService._format_json(data, include_metadata, metadata)
        elif format_type == "csv":
            return FormatterService._format_csv(data)
        elif format_type == "parquet":
            return FormatterService._format_parquet(data)
        else:
            raise ValueError(f"Formato de saída não suportado: {format_type}")
    
    @staticmethod
    def _format_json(data: List[Dict[str, Any]], include_metadata: bool = False, 
                    metadata: Optional[Dict[str, Any]] = None) -> str:
        """Formata os dados como JSON"""
        if include_metadata:
            result = {
                "data": data,
                "count": len(data),
                "metadata": metadata or {}
            }
            return json.dumps(result, ensure_ascii=False)
        return json.dumps(data, ensure_ascii=False)
    
    @staticmethod
    def _format_csv(data: List[Dict[str, Any]]) -> str:
        """Formata os dados como CSV"""
        if not data:
            return ""
            
        output = io.StringIO()
        fieldnames = list(data[0].keys())
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        
        return output.getvalue()
    
    @staticmethod
    def _format_parquet(data: List[Dict[str, Any]]) -> bytes:
        """Formata os dados como Parquet"""
        df = pd.DataFrame(data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer)
        return parquet_buffer.getvalue()


# Instância global do serviço para uso em toda a aplicação
formatter_service = FormatterService()