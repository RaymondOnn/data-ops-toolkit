``` mermaid

graph LR
    Input[Extract Stage Start] --> Count[Get Total Row Count]
    Count --> Width[Width Factor: Columns x Rows]
    Width --> Calc{Calculate Workers}
    Calc -->|Wide Data| Few[Fewer rows per worker]
    Calc -->|Narrow Data| Many[More rows per worker]
    Many --> Ray[Ray Resource Map: CPU/MEM/IO]

```
