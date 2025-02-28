🔷 **Jour 3 : Patterns Orientés Objet**

**Objectif :** Convertir le pipeline en structure orientée objet
**Exercice :**

Créer une classe `SalesPipeline` avec :

- `__init__(config)`
- `run()`

Utiliser l'héritage pour créer une classe de base `BasePipeline`

Implémenter le pattern Template Method

**Code Example :**

```python
class BasePipeline:
    def run(self):
        data = self.extract()
        transformed = self.transform(data)
        self.load(transformed)

class SalesPipeline(BasePipeline):
    def extract(self):
        ...
