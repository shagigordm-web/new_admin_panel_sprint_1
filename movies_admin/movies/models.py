import uuid

from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _

class TimeStampedMixin(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True

class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField(_('description'), blank=True)

    def __str__(self):
        return self.name 

    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = "content\".\"genre"
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'

class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work" 

class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full_name'), max_length=255)

    def __str__(self):
        return self.full_name

    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = "content\".\"person"
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = 'Актёра'
        verbose_name_plural = 'Актёрвов'

class PersonFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.TextField('Role', max_length=500)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work" 

class FilmWork(UUIDMixin, TimeStampedMixin):
    class Type(models.TextChoices):
        MOVIE = 'movie'
        TV_SHOW = 'tv_show'

    title = models.CharField(_('title'))
    description = models.TextField(_('description'), blank=True)
    file_path = models.URLField(_('file_path'), editable=False, null=True, blank=True)
    creation_date = models.DateField(_('creation_date'), blank=True)
    rating = models.FloatField(_('rating'), blank=True,
                            validators=[MinValueValidator(0),
                                        MaxValueValidator(100)]) 

    type = models.CharField(
        _('type'),
        max_length=10,
        choices=Type.choices,
        default=Type.MOVIE,
    )

    genres = models.ManyToManyField(Genre, through='GenreFilmWork')
    persons = models.ManyToManyField(Person, through='PersonFilmWork')
    
    def __str__(self):
        return self.title

    class Meta:
        # Ваши таблицы находятся в нестандартной схеме. Это нужно указать в классе модели
        db_table = "content\".\"film_work"
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = 'Фильмы'
        verbose_name_plural = 'Фильмы'