Основная идея утилиты - берем большой файл и делем его на пакеты величиной Х строк каждый пакет.

А потом обрабатываем каждый пакет в одной транзакции, с возможностью отката при не успехе.

Бонусом в папке import - открытые топы веб сайтов из разных источников.

Первоначально скрипт работал с Mongo DB. Но из коробки не удалось получить адекватные скорости при обработке файла на 1 млн. строк. Да-да индексы использовал.



Created from templates made available by Stagehand under a BSD-style
[license](https://github.com/dart-lang/stagehand/blob/master/LICENSE).
# packet_import
