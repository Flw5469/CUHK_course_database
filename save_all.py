from save import main
import glob

html_files = glob.glob("course_data/*.html")
print(html_files)
for html in html_files:
  main(html, "result.csv")