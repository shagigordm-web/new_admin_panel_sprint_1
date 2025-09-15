from django.contrib import admin
from .models import Genre
from .models import Person
from .models import FilmWork
from .models import GenreFilmWork
from .models import PersonFilmWork



@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    list_filter = ('name',)
    search_fields = ('name', 'description',)

class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork

class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (
        GenreFilmWorkInline,
        PersonFilmWorkInline
    )
    list_display = ('title', 'type', 'creation_date', 'rating',) 
    list_filter = ('type',)
    search_fields = ('title', 'description',)

@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    list_filter = ('full_name',)
    search_fields = ('full_name',)