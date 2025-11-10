import scrapy

class BillboardSpiderSpider(scrapy.Spider):
    name = 'billboard_spider'
    allowed_domains = ['en.wikipedia.org']
    start_urls = ['https://en.wikipedia.org/wiki/Billboard_Year-End']

    def parse(self, response):
        """
        Analyse la page d'index principale de Wikipedia.
        Trouve tous les liens vers les classements annuels et génère une nouvelle
        requête (Request) pour chacun, en appelant 'self.parse_song_page'.
        """
        self.log(f'Analyse de la page index principale : {response.url}')
        
        # Sélecteur CSS pour trouver les liens dans la navbox
        links_selector = 'div.navbox[aria-labelledby^="Billboard_Year-End_Hot_100_singles"] .navbox-list li a'
        links = response.css(links_selector)

        self.log(f'Trouvé {len(links)} liens d\'années à crawler.')

        for link in links:
            relative_url = link.css('::attr(href)').get()
            year_text = link.css('::text').get()

            # S'assurer qu'il s'agit d'un lien d'année valide
            if relative_url and year_text and year_text.strip().isdigit():
                full_url = response.urljoin(relative_url)
                
                # Au lieu de 'yield' le lien, nous 'yield' une nouvelle Requête
                # pour crawler cette page.
                # Le 'callback' indique à Scrapy quelle fonction appeler
                # une fois la page téléchargée.
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_song_page
                )
            else:
                self.log(f'Lien non-traité (pas une année) : {link.get()}')

    def parse_song_page(self, response):
        """
        Analyse une page de classement annuelle (ex: "..._of_1970").
        Cette fonction est appelée pour chaque lien trouvé dans 'parse()'.
        Elle scrape la table des chansons et 'yield' un dictionnaire par chanson.
        """
        self.log(f'Analyse de la page de chansons : {response.url}')

        # 1. Extraire l'année depuis le titre de la page (ex: <h1>)
        page_title = response.css('h1#firstHeading::text').get()
        year = "UNKNOWN" # Valeur par défaut
        if page_title and 'of ' in page_title:
            # Extrait l'année, ex: "1970" depuis "... singles of 1970"
            # Fonctionne aussi pour "... top 30 singles of 1949"
            year = page_title.split('of ')[-1].strip()
        
        # 2. Trouver la table des chansons
        # On cible la table qui a les classes 'wikitable' et 'sortable'
        table = response.css('table.wikitable.sortable')
        if not table:
            self.log(f"Aucune table 'wikitable sortable' trouvée sur {response.url}")
            return

        # 3. Récupérer toutes les lignes <tr> qui contiennent des <td>
        # (cela permet de sauter la ligne d'en-tête <th>)
        rows = table.css('tbody tr:has(td)')
        if not rows:
            self.log(f"Aucune ligne <tr> avec <td> trouvée dans la table sur {response.url}")
            return

        self.log(f'Trouvé {len(rows)} chansons pour l\'année {year}.')

        # 4. Boucler sur chaque ligne pour extraire les données
        for row in rows:
            # Récupérer toutes les cellules (colonnes) de la ligne
            cells = row.css('td')
            
            # S'assurer qu'on a bien au moins 3 cellules (No., Titre, Artiste)
            if len(cells) < 3:
                self.log(f'Ligne malformée sur {response.url}: {row.get()}')
                continue # Passer à la ligne suivante

            # --- Extraction des données ---
            
            # Colonne 'place': se trouve dans la 1ère cellule <td>
            place = cells[0].css('::text').get()
            
            # Colonne 'name': se trouve dans la 2ème cellule <td>
            # On joint tous les nœuds de texte (au cas où) et on enlève les guillemets
            name = "".join(cells[1].css('::text').getall()).strip().strip('"')
            
            # Colonne 'link': le 'href' du lien <a> dans la 2ème cellule <td>
            link_relative = cells[1].css('a::attr(href)').get()
            link = None # Par défaut, s'il n'y a pas de lien
            if link_relative:
                link = response.urljoin(link_relative) # Crée le lien absolu
            
            # Colonne 'artist': se trouve dans la 3ème cellule <td>
            # On joint tous les nœuds de texte pour gérer les artistes multiples
            # (ex: "Artiste A & Artiste B")
            artist = "".join(cells[2].css('::text').getall()).strip()

            # --- Yield un dictionnaire par chanson ---
            # Les clés ('name', 'artist', etc.) deviendront
            # automatiquement les en-têtes du fichier CSV.
            yield {
                'name': name,
                'artist': artist,
                'year': year,
                'place': place.strip() if place else None,
                'link': link
            }