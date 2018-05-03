## Documentation for `Processor.java`

See references within the code. Larger explanations have been moved here for code cleaning

### constant Value

get the constant value (if is was declared) to set the instance field to in case all
conditions are met for a rule, since there can be multiple rules
each with multiple conditions, a match of all conditions in a single rule
will set the instance's field to the const value. hence, it is an AND
between all conditions `pr.doBreak()` ?and an OR between all rules
example of a constant value declaration in a rule:
```
"rules": [
           {
             "conditions": [.....],
             "value": "book"
```
if this value is not indicated, the value mapped to the instance field will be the
output of the function - see below for more on that

### Functions

1..n functions can be declared within a condition (comma delimited).
for example:
A condition with with one function, a parameter that will be passed to the
function, and the expected value for this condition to be met
```
{
  "type": "char_select",
  "parameter": "0",
  "value": "7"
}
```
the functions here can rely on the single value field for comparison
to the output of all functions on the marc's field data
or, if a custom function is declared, the value will contain
the javascript of the custom function
for example:
```
"type": "custom",
"value": "DATA.replace(',' , ' ');"
```

### Delimiters

allow to declare a delimiter when concatenating subfields.
also allow , in a multi subfield field, to have some subfields with delimiter x and
some with delimiter y, and include a separator to separate each set of subfields
maintain a delimiter per subfield map - to lookup the correct delimiter and place it in string
maintain a string buffer per subfield - but with the string buffer being a reference to the
same stringbuilder for subfields with the same delimiter - the stringbuilder references are
maintained in the buffers2concat list which is then iterated over and we place a separator
between the content of each string buffer reference's content
