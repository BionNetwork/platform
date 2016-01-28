
var gulp = require('gulp'),
    watch = require('gulp-watch'),
    concat = require('gulp-concat'),
    minifyJS = require('gulp-uglify'),
    minifyCSS = require('gulp-minify-css'),
    minifyHTML = require('gulp-minify-html'),
    usemin = require('gulp-usemin');


var paths = {
  dist: 'dist/',
  scripts: 'src/**/*.js',
  htmlComponents: 'src/components/**/*.html', distHtmlComponents: 'dist/components/',
  htmlShared: 'src/shared/**/*.html', distHtmlShared: 'dist/shared/',
  htmlMain: 'src/main/**/*.html', distHtmlMain: 'dist/main',
  index: 'src/index.html'
};

gulp.task('usemin', function() {
  return gulp.src(paths.index)
    .pipe(
      usemin(
        {
          js: [
            //minifyJS(),
            'concat'
          ],
          css: [
            minifyCSS({
              keepSpecialComments: 0
            }),
            'concat'
          ],
        }
      )
    )
    //.pipe(minifyHTML())
    .pipe(gulp.dest(paths.dist));
});

gulp.task('htmlShared', function() {
  return gulp.src(paths.htmlShared)
    .pipe(minifyHTML())
    .pipe(gulp.dest(paths.distHtmlShared));
});

gulp.task('htmlComponents', function() {
  return gulp.src(paths.htmlComponents)
    .pipe(minifyHTML())
    .pipe(gulp.dest(paths.distHtmlComponents));
});

gulp.task('htmlMain', function() {
  return gulp.src(paths.htmlMain)
    .pipe(minifyHTML())
    .pipe(gulp.dest(paths.distHtmlMain));
});

gulp.task('scripts', function() {
  return gulp.src([
    paths.scripts
  ])
  //.pipe(minifyJS())
  .pipe(concat('main.js'))
  .pipe(gulp.dest(paths.dist));
});

gulp.task('watch', function() {
  gulp.watch([paths.htmlComponents], ['htmlComponents']);
  gulp.watch([paths.htmlShared], ['htmlShared']);
  gulp.watch([paths.htmlMain], ['htmlMain']);
  gulp.watch([paths.index], ['usemin']);
  gulp.watch([paths.scripts], ['scripts']);
});

gulp.task('default', ['usemin','htmlShared', 'htmlMain', 'htmlComponents', 'scripts', 'watch']);

