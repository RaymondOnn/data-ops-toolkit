class TypeNormalizationStrategy:
    def get_cast_map(self, schema: pl.Schema) -> list[pl.Expr]:
        exprs = []
        for col, dtype in schema.items():
            if dtype == pl.Boolean:
                # MSSQL Bit -> Int64
                exprs.append(pl.col(col).cast(pl.Int64, strict=True).alias(col))
            
            elif dtype == pl.String:
                # String Normalization (Trim + Strict Length)
                exprs.append(pl.col(col).str.strip_chars().alias(col))
            
            else:
                exprs.append(pl.col(col))
        return exprs