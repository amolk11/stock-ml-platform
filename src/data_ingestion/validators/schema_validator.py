"""
Schema Validator - Validate data structure and types
"""

from typing import Dict, Any, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validate market data schema and types."""
    
    REQUIRED_FIELDS = {
        'symbol': str,
        'timestamp': datetime,
        'open': (int, float),
        'high': (int, float),
        'low': (int, float),
        'close': (int, float),
        'volume': int,
        'source': str
    }
    
    @classmethod
    def validate_record(cls, record: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate a single market data record.
        
        Args:
            record: Market data dictionary
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required fields exist
        for field, expected_type in cls.REQUIRED_FIELDS.items():
            if field not in record:
                errors.append(f"Missing required field: {field}")
                continue
            
            # Check type
            if not isinstance(record[field], expected_type):
                errors.append(
                    f"Invalid type for {field}: expected {expected_type}, "
                    f"got {type(record[field])}"
                )
        
        # Business logic validation
        if 'open' in record and 'high' in record and 'low' in record and 'close' in record:
            if not (record['low'] <= record['open'] <= record['high']):
                errors.append("Open price outside low-high range")
            
            if not (record['low'] <= record['close'] <= record['high']):
                errors.append("Close price outside low-high range")
            
            if record['high'] < record['low']:
                errors.append("High price less than low price")
        
        if 'volume' in record and record['volume'] < 0:
            errors.append("Volume cannot be negative")
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    @classmethod
    def validate_batch(cls, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a batch of records.
        
        Args:
            records: List of market data dictionaries
            
        Returns:
            Validation summary
        """
        total = len(records)
        valid_count = 0
        invalid_records = []
        
        for idx, record in enumerate(records):
            is_valid, errors = cls.validate_record(record)
            
            if is_valid:
                valid_count += 1
            else:
                invalid_records.append({
                    'index': idx,
                    'record': record,
                    'errors': errors
                })
        
        summary = {
            'total_records': total,
            'valid_records': valid_count,
            'invalid_records': total - valid_count,
            'validation_rate': valid_count / total if total > 0 else 0,
            'errors': invalid_records[:10]  # Keep first 10 errors
        }
        
        if invalid_records:
            logger.warning(
                f"Validation: {valid_count}/{total} valid "
                f"({summary['validation_rate']:.1%})"
            )
        
        return summary