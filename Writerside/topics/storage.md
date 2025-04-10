# Встраиваемое ядро хранилища

## Обзор архитектуры

Ядро хранилища делится на несколько взаимосвязанных модулей:

### Внешний API (интерфейс Storage)

> Определяет операции чтения (get, итераторы по диапазонам) и записи (upsert), 
а также операции фонового сброса данных на диск (flush) и слияния таблиц (compaction).

API Storage (интерфейс хранилища) представляет из себя базовый интерфейс для LSM хранилища (хранилища
на базе журнально-структурированного дерева слияния).

#### Код интерфейса Storage {collapsible="true"}

<tabs>
    <tab title="API Storage">
        <code-block lang="java">
            <![CDATA[import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface Storage<D, E extends Entry<D>> extends Closeable {

    default E get(D key) {
        Iterator<E> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }

        E next = iterator.next();
        if (next.key().equals(key)) {
            return next;
        }
        return null;
    }

    default Iterator<E> allFrom(D from) {
        return get(from, null);
    }

    default Iterator<E> allTo(D to) {
        return get(null, to);
    }

    Iterator<E> get(D from, D to);

    default Iterator<E> all() {
        return get(null, null);
    }

    void upsert(E entry);

    default void flush() throws IOException {
        // Do nothing
    }

    default void compact() throws IOException {
        // Do nothing
    }

    @Override
    default void close() throws IOException {
        flush();
    }

}]]></code-block>
</tab>
</tabs>


<tabs>
    <tab title="Entry">
        <code-block lang="java">
            <![CDATA[public interface Entry<D> {
    D key();

    D value();
}

]]></code-block>
</tab>
</tabs>

### Модуль операций записи и чтения (LSMStorage)

> Основная реализация интерфейса, которая управляет всеми внутренними компонентами:

- **Memtable** – in-memory структура, реализующая упорядоченное хранилище данных при помощи ConcurrentSkipListMap, где происходят все операции вставки и обновления.

- **WAL (Write-Ahead Log)** – журнал операций, позволяющий сохранять последовательность изменений для восстановления при сбоях.

- **Фоновый модуль flush** – следит за размером Memtable и при превышении порога инициирует атомарный сброс Memtable на диск в виде нового SSTable.

### Модуль работы с SSTable (на диске)  

> Реализует чтение и запись на диск. Каждый SSTable представляет собой файл со следующей структурой:

- **Заголовок SSTable** – содержит метаданные: размер, имя, параметры фильтра Блюма, количество записей и т.д.

- **Фильтр Блюма** – используется для быстрой проверки наличия ключа в SSTable, исключая ненужные обращения к диску.

- **Оффсеты ключей** – массив значений (offsets) записей для бинарного поиска по ключам.

- **Данные записей** – последовательное хранение записей, где каждая запись состоит из:  
   1. Размер ключа 
   2. Собственно ключ
   3. Размер значения
   4. Собственно значение.

### Модуль compaction (слияния SSTable)

> Фоновый процесс, который периодически (либо при достижении определённых пороговых значений) обрабатывает набор SSTable, объединяя их в один новый файл. Это позволяет:

• Устранить дублирующиеся данные или устаревшие (tombstone) записи (например, при операциях обновления или удаления).

• Сократить общее количество файлов, улучшая производительность чтения.

## Архитектура

<code-block lang="plantuml">
@startuml "Компонентная архитектура (Блок-схема)"

title Компонентная архитектура

rectangle "Клиент / Приложение" as Client
rectangle "Хранилище LSMStorage\n(Реализация Storage)" as Storage

rectangle "Memtable\n(in-memory)" as Memtable
rectangle "WAL\n(Append-Only лог)" as WAL
rectangle "SSTable Manager\n(работа с файлами)" as SSTableMgr

rectangle "Фоновый Flush (async)" as Flush
rectangle "Модуль Compaction\n(слияние SSTable)" as Compaction

' Основной поток
Client --> Storage
Storage --> Memtable
Storage --> WAL
Storage -> SSTableMgr

' Связь для flush
Memtable --> Flush : "  При достижении порога"
Flush --> SSTableMgr : "  Создание новой SSTable"

' Связь для compaction
SSTableMgr --> Compaction : "  Фоновое слияние данных"

@enduml
</code-block>


<code-block lang="plantuml">
@startuml "Высокоуровневый поток обработки операций"
title Высокоуровневый поток обработки операций

rectangle "Клиент" as Client
rectangle "LSMStorage\n(реализация Storage)" as Storage
rectangle "Операция upsert" as Upsert
rectangle "WAL (Write-Ahead Log)" as WAL
rectangle "Memtable\n(in-memory структура)" as Memtable
rectangle "Отработка размера Memtable\n(Порог превышен?)" as Check
rectangle "Фоновый flush\n(перенос в SSTable)" as Flush
rectangle "Создание нового SSTable\n(запись на диск с помощью\nMemorySegment API)" as NewSSTable
rectangle "SSTables\n(на диске)" as SSTables
rectangle "Операция get" as Get
rectangle "Итерация по данным" as Iteration

' Связи
Client --> Storage
Storage --> Upsert
Upsert --> WAL : "Запись в WAL"
WAL --> Memtable : "Далее запись в\nMemtable"
Memtable --> Check : "Мониторинг размера"
Check --> Flush : "Если превышен"
Flush --> NewSSTable : "flush"
NewSSTable --> SSTables
SSTables --> Get
Get --> Iteration : "Чтение:\n1. Поиск в Memtable\n2. Если не найдено –\nитерация по SSTable\n(Filter, бинарный поиск)"
@enduml
</code-block>


## Структура SSTable

Структура SSTable в данном хранилище состоит из следующих компонентов.

<procedure title="Структура SSTable" id="sstable_structure">
    <step>
        <p>Заголовок SSTable </p>
    </step>
    <step>
        <p>Фильтр Блюма </p>
    </step>
    <step>
        <p>Оффсеты ключей </p>
    </step>
    <step>
        <p>Записи </p>
    </step>
</procedure>

### Структура заголовка SSTable

**Заголовок** представляет различные метаданные таблицы, а именно: 
размер битового массива фильтра Блюма, число хеш-функций, которое используется для
фильтра Блюма, количество записей, хранящихся в данном в SSTable.


| Размер, байт | 8                            | 8                 | 8                  |
|--------------|------------------------------|-------------------|--------------------|
| Имя          | Размер массива фильтра Блюма | Число хеш-функций | Количество записей |

### Структура фильтра Блюма SSTable

**Фильтр Блюма** используется для оптимизации чтения и предотвращает от лишнего чтения, если в данном
SSTable нет требуемого ключа. Состоит их хешей размером по 8 байт.

| Размер, байт | 8     | 8     | ... | 8     |
|--------------|-------|-------|-----|-------|
| Имя          | хеш 1 | хеш 2 | ... | хеш_n |

где n - размер массива фильтра Блюма.

### Структура оффсетов ключей SSTable

**Оффсеты ключей** используются для бинарного поиска в данном SSTable. Они стоят перед
самими данными. 

| Размер, байт | 8              | 8              | ... | 8              |
|--------------|----------------|----------------|-----|----------------|
| Имя          | оффсет ключа 1 | оффсет ключа 2 | ... | оффсет ключа n |

где n - количество записей в SSTable.

### Структура записей SSTable

**Записи** представляют из себя фактические данные. Запись состоит из размера ключа 8 байт, самого ключа,
размера записи 8 байт и самой записи. Ниже представлена структура хранения одной записи в SSTable:


| Размер, байт | 8            | Размер ключа | 8               | Размер значения |
|--------------|--------------|--------------|-----------------|-----------------|
| Имя          | Размер ключа | Ключ         | Размер значения | Значение        |



## ЯП и технологии

- Java 21: Использование последних возможностей языка.

- MemorySegment API: Для работы с off-heap памятью и эффективного управления байтовыми буферами. Применяется при манипуляциях с данными SSTable, WAL и при маппинге файлов.

- Пул потоков / ScheduledExecutorService: Для реализации фоновых задач (flush и compaction).

- Gradle для сборки




