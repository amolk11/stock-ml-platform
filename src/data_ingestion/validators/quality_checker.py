"""
Data Quality Checker - Check data quality metrics
"""

from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Check data quality and calculate quality scores."""
    
    @staticmethod
    def calculate_quality_score(record: Dict[str, Any]) -> float:
        """
        Calculate quality score for a market data record.
        
        Args:
            record: Market data dictionary
            
        Returns:
            Quality score between 0.0 and 1.0
        """
        score = 1.0
        
        # Check for None/null values in critical fields
        critical_fields = ['open', 'high', 'low', 'close', 'volume']
        for field in critical_fields:
            if record.get(field) is None:
                score -= 0.2
        
        # Check for zero volume (suspicious)
        if record.get('volume', 0) == 0:
            score -= 0.1
        
        # Check for zero prices
        if record.get('close', 0) == 0:
            score -= 0.3
        
        # Check for price anomalies (e.g., high/low too different)
        high = record.get('high', 0)
        low = record.get('low', 0)
        if high > 0 and low > 0:
            ratio = high / low
            if ratio > 1.5:  # More than 50% intraday range (suspicious)
                score -= 0.1
        
        return max(0.0, score)
    
    @classmethod
    def check_batch_quality(
        cls,
        records: List[Dict[str, Any]],
        min_quality_score: float = 0.8
    ) -> Dict[str, Any]:
        """
        Check quality of a batch of records.
        
        Args:
            records: List of market data dictionaries
            min_quality_score: Minimum acceptable quality score
            
        Returns:
            Quality check summary
        """
        total = len(records)
        quality_scores = []
        low_quality_records = []
        
        for idx, record in enumerate(records):
            score = cls.calculate_quality_score(record)
            quality_scores.append(score)
            
            # Add quality score to record
            record['data_quality_score'] = score
            
            if score < min_quality_score:
                low_quality_records.append({
                    'index': idx,
                    'symbol': record.get('symbol'),
                    'timestamp': record.get('timestamp'),
                    'score': score
                })
        
        avg_score = sum(quality_scores) / total if total > 0 else 0
        
        summary = {
            'total_records': total,
            'average_quality_score': avg_score,
            'low_quality_count': len(low_quality_records),
            'low_quality_records': low_quality_records[:10]
        }
        
        if low_quality_records:
            logger.warning(
                f"Quality: {len(low_quality_records)} records below "
                f"{min_quality_score} threshold (avg: {avg_score:.2f})"
            )
        
        return summary