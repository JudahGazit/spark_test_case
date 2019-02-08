# spark_test_case
<div dir="rtl">
הפרויקט הקטן הזה הוא מדגים של פתרון אפשרי לבעיית unittests המשתמשים בספארק.

כבר מזמן גילינו איך לעשות UT המשתמש בספארק בתוכו. לרוב התהליך נראה כך:
* Create input dataframe (in memory)
* Create expected dataframe (in memory)
* Execute function over the input dataframe
* Assert whether the result equals to the expected result
    * Compare sizes?
    * Join? Subtract?
    * Collect both?

UT בספארק הם דבר לא פשוט בכלל, מכמה סיבות:
* היעדר יכולות מובנות המאפשרות בדיקה של התוצאה המתקבלת מול התוצאה הצפויה
* זמן ריצה ארוך מאוד
    * יצירת הContext מהווה צוואר בקבוק
* תלות בסביבה חיצונית 

</div>