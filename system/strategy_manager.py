"""
Multi-Strategy Manager for OpenAlgo Trading System

This module provides a centralized strategy management system that allows:
1. Multiple strategies to be assigned to different stocks
2. Rule-based strategy assignment (market cap, sector, symbol-specific)
3. Dynamic strategy instantiation and management
4. Scalable configuration-driven approach

"""

import logging
import importlib
from typing import Dict, List, Any, Optional, Type
from logger import logger


class StrategyManager:
    """
    Manages multiple trading strategies and their assignment to symbols.
    
    Features:
    - Rule-based strategy assignment
    - Dynamic strategy loading and instantiation
    - Market cap, sector, and symbol-specific assignments
    - Fallback to default strategy
    - Caching for performance optimization
    """
    
    def __init__(self, strategy_config: Dict[str, Any]):
        """
        Initialize the Strategy Manager.
        
        Args:
            strategy_config: Configuration dictionary containing strategy rules and stock metadata
        """
        self.strategy_config = strategy_config
        self.default_strategy = strategy_config.get("default_strategy", "EnhancedTrendScoreStrategy")
        self.assignment_rules = strategy_config.get("assignment_rules", [])
        self.stocks_metadata = strategy_config.get("stocks", {})
        
        # Cache for strategy classes and instances
        self._strategy_classes: Dict[str, Type] = {}
        self._strategy_instances: Dict[str, Any] = {}
        self._symbol_strategy_mapping: Dict[str, str] = {}
        
        logger.info(f"StrategyManager initialized with {len(self.assignment_rules)} rule sets")
        
        # Pre-compute strategy assignments for all configured stocks
        self._precompute_strategy_assignments()
    
    def _precompute_strategy_assignments(self):
        """Pre-compute strategy assignments for all configured stocks."""
        for symbol in self.stocks_metadata.keys():
            strategy_name = self.get_strategy_for_symbol(symbol)
            self._symbol_strategy_mapping[symbol] = strategy_name
            logger.debug(f"Pre-assigned {symbol} -> {strategy_name}")
    
    def _load_strategy_class(self, strategy_name: str) -> Type:
        """
        Dynamically load a strategy class by name.
        
        Args:
            strategy_name: Name of the strategy class to load
            
        Returns:
            Strategy class type
            
        Raises:
            ImportError: If strategy cannot be imported
            AttributeError: If strategy class not found in module
        """
        if strategy_name in self._strategy_classes:
            return self._strategy_classes[strategy_name]
        
        try:
            # Strategy module mapping
            strategy_modules = {
                "EnhancedTrendScoreStrategy": "strategy.trendscore",
                "LargeCapStrategy": "strategy.largecap_strategy", 
                "MidCapStrategy": "strategy.midcap_strategy",
                "SmallCapStrategy": "strategy.smallcap_strategy"
            }
            
            module_name = strategy_modules.get(strategy_name)
            if not module_name:
                raise ImportError(f"Unknown strategy: {strategy_name}")
            
            # Import the module and get the class
            module = importlib.import_module(module_name)
            strategy_class = getattr(module, strategy_name)
            
            # Cache the class
            self._strategy_classes[strategy_name] = strategy_class
            logger.info(f"Successfully loaded strategy class: {strategy_name}")
            
            return strategy_class
            
        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to load strategy {strategy_name}: {e}")
            # Fallback to default strategy
            if strategy_name != self.default_strategy:
                logger.warning(f"Falling back to default strategy: {self.default_strategy}")
                return self._load_strategy_class(self.default_strategy)
            else:
                raise ImportError(f"Cannot load default strategy {self.default_strategy}: {e}")
    
    def get_strategy_for_symbol(self, symbol: str) -> str:
        """
        Determine which strategy should be used for a given symbol.
        
        Args:
            symbol: Stock symbol (e.g., "RELIANCE", "INFY")
            
        Returns:
            Strategy class name to use for this symbol
        """
        # Check cache first
        if symbol in self._symbol_strategy_mapping:
            return self._symbol_strategy_mapping[symbol]
        
        # Remove NSE: prefix if present for lookup
        clean_symbol = symbol.replace("NSE:", "").replace("-EQ", "")
        
        # Get stock metadata
        stock_metadata = self.stocks_metadata.get(clean_symbol, {})
        
        # Check for direct strategy override in stock config
        if "strategy" in stock_metadata:
            strategy_name = stock_metadata["strategy"]
            logger.debug(f"Direct strategy override for {symbol}: {strategy_name}")
            return strategy_name
        
        # Evaluate assignment rules in order - collect all matches and use the last one
        matched_strategy = None
        
        for rule_set in self.assignment_rules:
            if not rule_set.get("enabled", True):
                continue
                
            rule_name = rule_set.get("name", "unnamed")
            rules = rule_set.get("rules", [])
            
            for rule in rules:
                condition = rule.get("condition", {})
                strategy_name = rule.get("strategy")
                
                if self._evaluate_condition(condition, clean_symbol, stock_metadata):
                    logger.debug(f"Rule '{rule_name}' matched for {symbol}: {strategy_name}")
                    matched_strategy = strategy_name  # Keep updating - last match wins
        
        # Return the last matched strategy or default
        if matched_strategy:
            return matched_strategy
        
        # Fallback to default strategy
        logger.debug(f"No rules matched for {symbol}, using default: {self.default_strategy}")
        return self.default_strategy
    
    def _evaluate_condition(self, condition: Dict[str, Any], symbol: str, metadata: Dict[str, Any]) -> bool:
        """
        Evaluate whether a condition matches for a given symbol.
        
        Args:
            condition: Condition dictionary to evaluate
            symbol: Stock symbol
            metadata: Stock metadata
            
        Returns:
            True if condition matches, False otherwise
        """
        for key, expected_value in condition.items():
            if key == "symbol":
                if symbol != expected_value:
                    return False
            elif key in metadata:
                if metadata[key] != expected_value:
                    return False
            else:
                # Key not found in metadata, condition fails
                return False
        
        return True
    
    def create_strategy_instance(self, symbol: str, **kwargs) -> Any:
        """
        Create a strategy instance for a given symbol.
        
        Args:
            symbol: Stock symbol
            **kwargs: Additional arguments for strategy initialization
            
        Returns:
            Strategy instance
        """
        # Check if we already have an instance for this symbol
        instance_key = f"{symbol}"
        if instance_key in self._strategy_instances:
            return self._strategy_instances[instance_key]
        
        # Get strategy name and load class
        strategy_name = self.get_strategy_for_symbol(symbol)
        strategy_class = self._load_strategy_class(strategy_name)
        
        # Create instance
        try:
            # Add symbol to kwargs for strategy initialization
            kwargs['symbol'] = symbol
            instance = strategy_class(**kwargs)
            
            # Cache the instance
            self._strategy_instances[instance_key] = instance
            logger.info(f"Created {strategy_name} instance for {symbol}")
            
            return instance
            
        except Exception as e:
            logger.error(f"Failed to create {strategy_name} instance for {symbol}: {e}")
            # Try to create default strategy instance
            if strategy_name != self.default_strategy:
                logger.warning(f"Falling back to default strategy for {symbol}")
                default_class = self._load_strategy_class(self.default_strategy)
                instance = default_class(**kwargs)
                self._strategy_instances[instance_key] = instance
                return instance
            else:
                raise
    
    def get_strategy_assignment_summary(self) -> Dict[str, List[str]]:
        """
        Get a summary of strategy assignments.
        
        Returns:
            Dictionary mapping strategy names to lists of assigned symbols
        """
        summary = {}
        
        for symbol in self.stocks_metadata.keys():
            strategy_name = self.get_strategy_for_symbol(symbol)
            if strategy_name not in summary:
                summary[strategy_name] = []
            summary[strategy_name].append(symbol)
        
        return summary
    
    def get_supported_strategies(self) -> List[str]:
        """
        Get list of supported strategy names.
        
        Returns:
            List of strategy class names
        """
        return [
            "EnhancedTrendScoreStrategy",
            "LargeCapStrategy", 
            "MidCapStrategy",
            "SmallCapStrategy"
        ]
    
    def validate_configuration(self) -> Dict[str, Any]:
        """
        Validate the strategy configuration.
        
        Returns:
            Validation result with status and any issues found
        """
        issues = []
        warnings = []
        
        # Check if default strategy is valid
        supported_strategies = self.get_supported_strategies()
        if self.default_strategy not in supported_strategies:
            issues.append(f"Default strategy '{self.default_strategy}' is not supported")
        
        # Check strategy assignments
        assignment_summary = self.get_strategy_assignment_summary()
        
        for strategy_name in assignment_summary.keys():
            if strategy_name not in supported_strategies:
                issues.append(f"Strategy '{strategy_name}' is not supported")
        
        # Check for symbols without metadata
        configured_symbols = set(self.stocks_metadata.keys())
        # This would typically come from trading_setting.symbols, but we'll assume it's available
        
        if not configured_symbols:
            warnings.append("No stock metadata configured")
        
        # Try to load all assigned strategies
        for strategy_name in assignment_summary.keys():
            try:
                self._load_strategy_class(strategy_name)
            except Exception as e:
                issues.append(f"Cannot load strategy '{strategy_name}': {e}")
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
            "assignment_summary": assignment_summary
        }
    
    def clear_cache(self):
        """Clear all cached strategy instances and classes."""
        self._strategy_instances.clear()
        self._strategy_classes.clear()
        self._symbol_strategy_mapping.clear()
        logger.info("Strategy manager cache cleared")
    
    def update_configuration(self, new_config: Dict[str, Any]):
        """
        Update strategy configuration and clear cache.
        
        Args:
            new_config: New strategy configuration
        """
        self.strategy_config = new_config
        self.default_strategy = new_config.get("default_strategy", "EnhancedTrendScoreStrategy")
        self.assignment_rules = new_config.get("assignment_rules", [])
        self.stocks_metadata = new_config.get("stocks", {})
        
        # Clear cache and recompute assignments
        self.clear_cache()
        self._precompute_strategy_assignments()
        
        logger.info("Strategy configuration updated")


# Utility functions for easy integration

def create_strategy_manager(config: Dict[str, Any]) -> StrategyManager:
    """
    Create and validate a StrategyManager instance.
    
    Args:
        config: Full configuration dictionary
        
    Returns:
        Configured StrategyManager instance
        
    Raises:
        ValueError: If configuration is invalid
    """
    strategy_config = config.get("strategy_config", {})
    
    if not strategy_config:
        raise ValueError("No strategy_config found in configuration")
    
    manager = StrategyManager(strategy_config)
    
    # Validate configuration
    validation = manager.validate_configuration()
    if not validation["valid"]:
        error_msg = "Strategy configuration validation failed:\n" + "\n".join(validation["issues"])
        raise ValueError(error_msg)
    
    # Log warnings if any
    for warning in validation["warnings"]:
        logger.warning(f"Strategy configuration warning: {warning}")
    
    # Log assignment summary
    assignment_summary = validation["assignment_summary"]
    logger.info("Strategy assignment summary:")
    for strategy_name, symbols in assignment_summary.items():
        logger.info(f"  {strategy_name}: {', '.join(symbols)}")
    
    return manager


def get_strategy_for_symbol_simple(symbol: str, config: Dict[str, Any]) -> str:
    """
    Simple utility to get strategy name for a symbol without creating a full manager.
    
    Args:
        symbol: Stock symbol
        config: Configuration dictionary
        
    Returns:
        Strategy class name
    """
    manager = create_strategy_manager(config)
    return manager.get_strategy_for_symbol(symbol) 