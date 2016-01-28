
var gulp = require('gulp'),
    watch = require('gulp-watch'),
    //concat = require('gulp-concat'),
    minifyJS = require('gulp-uglify'),
    minifyCSS = require('gulp-minify-css'),
    usemin = require('gulp-usemin');


var paths = {
  dist: 'dist/',
  index: 'src/index.html'
};

gulp.task('usemin', function() {
  return gulp.src(paths.index)
    .pipe(
      usemin(
        {
          js: [
            minifyJS(),
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
    .pipe(gulp.dest('dist/'));
});

gulp.task('watch', function() {
  gulp.watch([paths.index], ['usemin']);
});

gulp.task('default', ['usemin', 'watch']);

