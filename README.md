# spark_test_case
<div dir="rtl">
הפרויקט הקטן הזה הוא מדגים של פתרון אפשרי לבעיית unittests המשתמשים בספארק.
כבר מזמן גילינו איך לעשות UT המשתמש בספארק בתוכו. לרוב התהליך נראה כך:
</div>


* Create input dataframe (in memory)
* Create expected dataframe (in memory)
* Execute function over the input dataframe
* Assert whether the result equals to the expected result
    * Compare sizes?
    * Join? Subtract?
    * Collect both?

<div dir="rtl">
UT בספארק הם דבר לא פשוט בכלל, מכמה סיבות:


* היעדר יכולות מובנות המאפשרות בדיקה של התוצאה המתקבלת מול התוצאה הצפויה
* זמן ריצה ארוך מאוד
    * יצירת הContext מהווה צוואר בקבוק
* תלות בסביבה חיצונית 


הפרויקט הזה מציג שיטה שמאפשרת הרצה של UT בצורה מהירה פי 100 (!) מהצורה הנאיבית שבה אנחנו משתמשים היום.
איך זה קורה?


* שימוש spark local במקום יצירת Context "אמיתי"
* Context לא נסגר -
לא מריצים UT דרך pycharm יותר, אלא דרך קובץ main מיוחד שישאיר את הContext באוויר. כדי להריץ מחדש UT לא צריך להריץ כלום, רק ללחוץ Enter. 
כמובן שזה יודע להתייחס לשינוי בCodeBase בין הרצה להרצה.



בנוסף, TestCase מיועד ל Spark מכיל בדיקה של תוכן הטבלאות בבדיקה (כולל הירככיה), ובודק (אם רוצים) גם את סדר העמודות והסוג שלהן בנפרד.


שני אלו ביחד מאפשרים הרצה של UT אמינים במהירות גבוהה מאוד, מה שעשוי לאפשר קפיצת מדרגה בזמן כתיבת הקוד.
במקום שיקח 10 שניות להרצת TEST אחד, כיוון שה Context נשמר - לוקח 0.6 שניות להריץ 5 בדיקות!
</div>