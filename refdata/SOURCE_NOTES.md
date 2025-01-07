# Refdata Source Notes

## IMSLP

Identifying JSON data on category pages:

```
<script>if(typeof catpagejs=='undefined')catpagejs={};$.extend(catpagejs,{__JSON_DATA__});if(typeof origcatmap=='undefined')origcatmap={};$.extend(origcatmap,{"s1":"Performers"});</script>
```

Constructing wiki page URLs from category page entry:

"https://imslp.org/wiki/\<URL encoded title>"

### Composers

JSON:

```json
{
  "s1": {
    "0": [
      "Anonymous",
      "Collections",
      "Traditional",
      "Various"
    ],
     .
     .
     .
    "V": [
      "V. P.",
      "Vaas, A.J.",
      "Vaccai, Nicola",
      "Vaccani, Luigi Maria",
      "Vacchelli, Giovanni Battista",
      "Vachon, Pierre",
      "Vadon, Jean",
      "Vaes, Gaspard",
      "Vaet, Jacobus",
      "Vagedes, Adolph von",
      "Vagnetti, Angelo",
      "Vágvölgyi, Béla",
      "Vaitzman, Or",
      "La Val",
       .
       .
       .
    ],
     .
     .
     .
  }
}
```

### Compositions

"Pages\_with\_commercial\_recordings"

JSON:

```json
{
  "p1": {
    "'": [
      "'E spingole frangese! (De Leva, Enrico)|NNaxos\\0|SScores\\1",
      "'O marenariello (Gambardella, Salvatore)|NNaxos\\0|SScores\\2",
      "'O sole mio (Di Capua, Eduardo)|RRecordings\\1|NNaxos\\0|SScores\\6|AArrangements and Transcriptions\\10",
      "'O surdato 'nnammurato (Cannio, Enrico)|NNaxos\\0|SScores\\1",
      "'O surdato sbruffone (Cannio, Enrico)|NNaxos\\0|SScores\\1"
    ],
     .
     .
     .
    "V": [
      "V mlhách (Janáček, Leoš)|NNaxos\\0|SScores\\2",
      "V podvečer, Op.39 (Fibich, Zdeněk)|NNaxos\\0|SFull Scores\\2|PParts\\13|AArrangements and Transcriptions\\4",
      "Va, dal furor portata, K.21/19c (Mozart, Wolfgang Amadeus)|NNaxos\\0|SScores\\2",
      "Vaaren – Vaaren er i Brudd!, CNW 353 (Nielsen, Carl)|NNaxos\\0|SScores\\1",
      "Vadam et circuibo (Victoria, Tomás Luis de)|NNaxos\\0|SScores\\2",
      "Vado ben spesso cangiando loco (Bononcini, Giovanni)|NNaxos\\0|SScores\\1",
      "Vado dal piano al monte (Leo, Leonardo)|NNaxos\\0|SScores\\2",
      "Vado, ma dove?, K.583 (Mozart, Wolfgang Amadeus)|NNaxos\\0|SFull Scores\\2|PParts\\7|VVocal Scores\\1",
      "Vaga luna che inargenti (Bellini, Vincenzo)|NNaxos\\0|SScores\\5",
      "Vaghi augelletti che d'amor formate (Conti, Francesco Bartolomeo)|NNaxos\\0|SScores\\1",
      "Vaghi augelletti, che per valli e monti (Gabrieli, Andrea)|NNaxos\\0",
      "Vaghi boschetti di soavi allori (Wert, Giaches de)|NNaxos\\0|SScores and Parts\\3|PScores and Parts\\3",
      "Vaillance (Ascher, Joseph)|NNaxos\\0|SScores\\3|AArrangements and Transcriptions\\1",
       .
       .
       .
    ],
     .
     .
     .
  }
}
```

## CLMU

### Composer

HTML:

```html
<div class="view-content">
  <table class="views-table cols-2"  summary="Table of Browse Composers for Music Online: Classical Music Library." tabindex="0">
    <thead>
      <tr>
        <th scope="col" class="views-field views-field-name active">
          Composer
        </th>
        <th scope="col" class="views-field views-field-count">
          <a href="/clmu/browse/composer?items_per_page=100&amp;page=10&amp;order=count" title="sort by Works by">Works by</a>
        </th>
      </tr>
    </thead>
    <tbody>
      <tr class="even views-row-first">
        <td class="views-field views-field-name active" >
          Bouzignac, Guillaume, 1587-1643
        </td>
        <td class="views-field views-field-count" >
          <a href="/clmu/browse/composition?ff%5B0%5D=composer_facet%3Abouzignac%20guillaume%2015871643%2ABouzignac%2C%20Guillaume%2C%201587-1643%7C201963">5</a>
        </td>
      </tr>
      <tr class="odd">
        <td class="views-field views-field-name active" >
          Bowen, York, 1884-1961
        </td>
        <td class="views-field views-field-count" >
          <a href="/clmu/browse/composition?ff%5B0%5D=composer_facet%3Abowen%20york%2018841961%2ABowen%2C%20York%2C%201884-1961%7C197913">33</a>
        </td>
      </tr>
      <tr class="odd">
        <td class="views-field views-field-name active" >
          Bristow, George Frederick, 1825-1898
        </td>
        <td class="views-field views-field-count" >
          <a href="/clmu/browse/composition?ff%5B0%5D=composer_facet%3Abristow%20george%20frederick%2018251898%2ABristow%2C%20George%20Frederick%2C%201825-1898%7C58763">1</a>
        </td>
      </tr>
    </tbody>
  </table>
</div>
```

### Composition

HTML:

```html
<div class="view-content">
  <div class="views-row views-row-90 views-row-even">
    <div class="lazr-browse-composition-item" title="Symphony" genre="Symphony" composed="1992">
      <div class="lazr-browse-composition-title">
        <span data-field="name">Symphony</span>
      </div>
      <div class="lazr-browse-composition-composer">
        Composer: <span>George Frederick Bristow, 1825-1898</span>
      </div>
      <div class="lazr-browse-composition-content">
        <ul class="lazr-browse-composition-performances" id="performances-90">
          <li>
            <div class="tree-expander tree-expanded">
              <b>
                <span data-field="real_title">Symphony in F Sharp Minor, Op. 26</span>
              </b>
              , performed by
              <span data-field="performing_body">Detroit Symphony Orchestra</span>
              ;
              <span data-field="group_parent_link">from CD <a href="/clmu/view/work/2227570">Samuel Barber: Symphony No. 2|Adagio for Strings; George Frederick Bristow: Symphony in F sharp minor</a></span>
              (
              <span data-field="publishing_body">Chandos</span>
              )
            </div>
            <ul class="lazr-browse-composition-tracks " style="display: block;">
              <li class="">
                <div class="cite-checkbox-and-playicon">
                  <a href="/clmu/view/work/2323340"><i class="fa fa-play-circle"></i></a>
                </div>
                <div class="cite-playlink"><a href="/clmu/view/work/2323340">
                  <span data-field="real_title">I. Allegro</span></a>
                </div>
              </li>
            </ul>
          </li>
          <li>
            <a href="/browse/composition/nojs/?items_per_page=100&amp;f%5B0%5D=object_id%3AMetawork%7C2323362&amp;grouplimit=4&amp;ajax=1" class="browse-use-ajax lazr-browse-composition-show-all" wrapper="performances-90">
              Show all<i class="fa fa-sort-down"></i>
            </a>
          </li>
        </ul>
      </div>
    </div>
  </div>
</div>
```

### Person

HTML:

```html
<div class="view-content">
  <table class="views-table cols-3"  summary="Table of Browse People for Music Online: Classical Music Library." tabindex="0">
    <thead>
      <tr>
        <th scope="col" class="views-field views-field-name active">
          Name
        </th>
        <th scope="col" class="views-field views-field-another-facet-column-one">
          Works by
        </th>
        <th scope="col" class="views-field views-field-another-facet-column-two">
          Works about
        </th>
      </tr>
    </thead>
    <tbody>
      <tr class="even views-row-first">
        <td class="views-field views-field-name active" >
          Brett, Charles
        </td>
        <td class="views-field views-field-another-facet-column-one" >
          <a href="/clmu/search?ff%5B0%5D=works_by_facet%3Abrett%20charles%2ABrett%2C%20Charles%7C205902&amp;ff%5B1%5D=person_facet%3Abrett%20charles%2ABrett%2C%20Charles%7C205902">7</a>
        </td>
        <td class="views-field views-field-another-facet-column-two" >
        </td>
      </tr>
      <tr class="odd">
        <td class="views-field views-field-name active" >
          Brister, Wanda, 1957-
        </td>
        <td class="views-field views-field-another-facet-column-one" >
          <a href="/clmu/search?ff%5B0%5D=works_by_facet%3Abrister%20wanda%2000001957%2ABrister%2C%20Wanda%2C%201957-%7C264138&amp;ff%5B1%5D=person_facet%3Abrister%20wanda%2000001957%2ABrister%2C%20Wanda%2C%201957-%7C264138">1</a>
        </td>
        <td class="views-field views-field-another-facet-column-two" >
        </td>
      </tr>
      <tr class="even">
        <td class="views-field views-field-name active" >
          Bristow, George Frederick, 1825-1898
        </td>
        <td class="views-field views-field-another-facet-column-one" >
          <a href="/clmu/search?ff%5B0%5D=works_by_facet%3Abristow%20george%20frederick%2018251898%2ABristow%2C%20George%20Frederick%2C%201825-1898%7C58763&amp;ff%5B1%5D=person_facet%3Abristow%20george%20frederick%2018251898%2ABristow%2C%20George%20Frederick%2C%201825-1898%7C58763">1</a>
        </td>
        <td class="views-field views-field-another-facet-column-two" >
        </td>
      </tr>
    </tbody>
  </table>
</div>
```


### Performer/Ensemble

HTML:

```html
<div class="view-content">
  <table class="views-table cols-2"  summary="Table of Browse Performers / Ensembles for Music Online: Classical Music Library." tabindex="0">
    <thead>
      <tr>
        <th scope="col" class="views-field views-field-name active">
          Performer / Ensemble
        </th>
        <th scope="col" class="views-field views-field-count">
          <a href="/clmu/browse/performer-ensemble?items_per_page=100&amp;page=100&amp;order=count" title="sort by Related works">Related works</a>
        </th>
      </tr>
    </thead>
    <tbody>
      <tr class="even views-row-first">
        <td class="views-field views-field-name active" >
          Kammerchor-St.-Andreas Hildesheim
        </td>
        <td class="views-field views-field-count" >
          <a href="/clmu/search?ff%5B0%5D=performing_body_facet%3AKammerchor-St.-Andreas%20Hildesheim">1</a>
        </td>
      </tr>
      <tr class="even">
        <td class="views-field views-field-name active" >
          Kapralova Quartet
        </td>
        <td class="views-field views-field-count" >
          <a href="/clmu/search?ff%5B0%5D=performing_body_facet%3AKapralova%20Quartet">2</a>
        </td>
      </tr>
      <tr class="odd">
        <td class="views-field views-field-name active" >
          Karen Clark, fl. 1995
        </td>
        <td class="views-field views-field-count" >
          <a href="/clmu/search?ff%5B0%5D=performing_body_facet%3AKaren%20Clark%2C%20fl.%201995">1</a>
        </td>
      </tr>
      <tr class="even">
        <td class="views-field views-field-name active" >
          Karen Click
        </td>
        <td class="views-field views-field-count" >
          <a href="/clmu/search?ff%5B0%5D=performing_body_facet%3AKaren%20Click">1</a>
        </td>
      </tr>
    </tbody>
  </table>
</div>
```
