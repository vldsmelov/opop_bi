# Дашборд ОПиОП

Веб-проект для построения дашборда по данным из `data/data.xlsx`.

## Быстрый старт

```bash
docker compose up --build -d
```

Открыть:

- `http://localhost:18000/` — главная.
- `http://localhost:18000/dashboard` — основной дашборд.
- `http://localhost:18000/calculation-services-debug` — тех. страница расчётов.
- `http://localhost:18000/excel-debug` — просмотр листов Excel.

## Git flow

- `main` — стабильная ветка.
- `develop` — интеграционная ветка разработки.
- `feature/<name>` — задачи.

Пример цикла:

```bash
git switch develop
git switch -c feature/my-task
# изменения...
git add -A
git commit -m "feat: my task"
git switch develop
git merge --no-ff feature/my-task
```
