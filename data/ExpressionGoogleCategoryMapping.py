"""
See link for more info:
 https://developers.google.com/places/supported_types
"""

CORE_CATEGORY_MAP = {
    "Arts & Crafts": 'store',
    "Bakers": 'bakery',
    "Bicycles": 'bicycle_store',
    "Bookstore": 'book_store',
    "Caf\u00e9": 'cafe',
    "Camera Shop": 'electronics_store',
    "Clothing (designer)": 'clothing_store',
    "Clothing, including (non designer labels)": '',
    "Delicatessen": '',
    "Electrical Goods - Audio Visual Goods - Hiring / Sale / Supply (ex cover whilst on hire)": 'electronics_store',
    "Florist ": 'florist',
    "Furniture Shops": 'furniture_store',
    "Gift / Fancy Goods Shop / Novelty": 'home_goods_store',
    "Greeting Card shop": 'store',
    "Health Shop / Organic foods": '',
    "Musical Instruments": 'store',
    "Office Furniture Retail (excluding computers and printers)": '',
    "Pet Shop / Pet Accessory Store": 'pet_store',
    "Restaurants - Other": ['meal_delivery', 'meal_takeaway', 'restaurant'],
    "Stationers": 'store',
    "Textiles, Bedding, Drapers and Soft Furnishings": 'home_goods_store',
    "Confectionery / Sweet Shop": '',
    "Ice Cream Parlour No Cooking": '',
    "Toy/Model shop (excluding Computer Game or consoles)": '',
    "Grocers (excluding wines/spirits/tobacco)": 'convenience_store',
    "Grocers (inc wines/spirits/tobacco)": 'convenience_store',
    "Beauticians (ex treatment)": "beauty_salon",
    "Hairdressers (ex treatment)": "hair_care",
    "Barbers / Beards / Men's grooming": "hair_care",
    "Hotels": "lodging",
    "Shoe Shop": "shoe_store",
    "Travel Agents": 'travel_agency',
    'Electrician': 'electrician',
    'Plumber': 'plumber',
    'Painter': 'painter'
}

"""
/ accounting
/ airport
/ amusement_park
/ aquarium
art_gallery
/ atm
+ bakery
/ bank
/ bar
beauty_salon
+ bicycle_store
+ book_store
/ bowling_alley
/ bus_station
+ cafe
/ campground
/ car_dealer
/ car_rental
/ car_repair
/ car_wash
/ casino
/ cemetery
/ church
/ city_hall
++ clothing_store
convenience_store
/ courthouse
/ dentist
department_store
/ doctor
+ drugstore
/ electrician
++ electronics_store
/ embassy
/ fire_station
+ florist
/ funeral_home
+ furniture_store
/ gas_station
/ gym
? hair_care
hardware_store
/ hindu_temple
++ home_goods_store
/ hospital
/ insurance_agency
/ jewelry_store
/ laundry
/ lawyer
/ library
/ light_rail_station
liquor_store
/ local_government_office
/ locksmith
/ lodging
meal_delivery
meal_takeaway
/ mosque
/ movie_rental
/ movie_theater
/ moving_company
/ museum
/ night_club
? painter
/ park
/ parking
+ pet_store
? pharmacy
/ physiotherapist
/ plumber
/ police
/ post_office
/ primary_school
/ real_estate_agency
+ restaurant
/ roofing_contractor
/ rv_park
/ school
/ secondary_school
++ shoe_store
shopping_mall
/ spa
/ stadium
/ storage
+ store
/ subway_station
supermarket
/ synagogue
/ taxi_stand
/ tourist_attraction
/ train_station
/ transit_station
/ travel_agency
/ university
veterinary_care
/ zoo
"""
