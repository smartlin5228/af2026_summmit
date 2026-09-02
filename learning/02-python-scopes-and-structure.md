# 02 — Python: organization levels vs. scope levels

Two different hierarchies that are easy to conflate.

---

## A. Organization levels (how code is grouped)

| Level | What it is | Example |
|---|---|---|
| **Distribution** ("library") | what you `pip install` (PyPI term: "distribution package") | `apache-airflow` |
| **Package** | a directory you can `import` — has `__init__.py`, or is a PEP-420 namespace package | `airflow`, `airflow.models` |
| **Module** | one `.py` file — the unit of `import` | `airflow/models/dag.py` → `airflow.models.dag` |
| **Class** | `class X:` — groups data + behavior | `class DAG:` |
| **Function / method** | `def f():` — a *method* is just a function defined inside a class | `def execute(self):` |
| **Block** | `if` / `for` / `while` / `with` / `try` | — |

One distribution can ship many packages; one package contains many modules; a
module contains classes and functions; classes contain methods.

---

## B. Scope levels (how Python resolves a name): **LEGB**

When Python encounters a name, it searches these scopes **in order**:

| | Scope | Where |
|---|---|---|
| **L** | Local | inside the current function call |
| **E** | Enclosing | outer function(s) this one is nested in (closures) |
| **G** | Global | **the module's top level** — "module scope" == "global scope" |
| **B** | Built-in | `print`, `len`, `range`, `Exception`, … |

There are **exactly four**. Assignment creates the name in the *current* scope
(Local if in a function, Global if at module level) unless `global` / `nonlocal`
says otherwise.

### The big gotcha: blocks are NOT scopes

`if` / `for` / `while` / `with` / `try` do **not** introduce a scope.

```python
for i in range(3):       # module level
    x = i * 2
print(x)                 # 4  — x leaked out; it's a module global
```

```python
def f():
    for i in range(3):
        y = i * 2
    return y             # fine — y is local to f(), loop didn't scope it
```

(Coming from C / Java / JavaScript-`let`, this surprises people.)

### Other subtleties

- **Class body is a scope, but methods don't see it.** A method needs `self.attr`
  or `ClassName.attr` — it can't read a bare class-body variable.
  ```python
  class C:
      TABLE = "users"
      def q(self):
          return f"select * from {TABLE}"   # NameError — need self.TABLE / C.TABLE
  ```
- **Comprehensions / generator exprs / lambdas have their own scope** (Py3).
  `[i for i in range(3)]` does not leak `i`.
- `global x` — lets a function rebind a module-global. `nonlocal x` — lets a
  nested function rebind a name in the enclosing function.

---

## Tie-back to DAG parsing

- "module scope" = "global scope" = the **G** in LEGB = **code at indent 0**.
- That code is the module body — it **runs when the module is imported**.
- Airflow imports each DAG file every parse cycle → module-level code runs every
  ~30s. Code inside a task's `def` does not (it runs only when the task executes,
  in a different process).
- See `01-python-imports-and-dag-parsing.md`.
