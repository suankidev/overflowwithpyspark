from bs4 import BeautifulSoup

my_html = '''
<!DOCTYPE html>
<html>
<body>

<h1>My First Heading</h1>
<p>My first paragraph.</p>
<p class="subtitile">My first paragraph.</p>
<ul>
<li>sujeet</li>
<li>ramesh</li>
<li>sham</li>
</ul>

</body>
</html>
'''

simple_soup = BeautifulSoup(my_html, 'html.parser')

h1_tag = simple_soup.find('h1')

print(h1_tag.string)

list_items = [name.string for name in simple_soup.find_all('li')]
print(list_items)


# looking for paragraph without class

def find_subtitile():
    paragraph = simple_soup.find('p', {'class': 'subtitile'})
    print(paragraph)


def find_other_paragraph():
    paragraphs = simple_soup.find_all('p')
    other_paragraph = [p for p in paragraphs if 'subtitile' not in p.attrs.get('class', [])]
    print(other_paragraph)


find_other_paragraph()
