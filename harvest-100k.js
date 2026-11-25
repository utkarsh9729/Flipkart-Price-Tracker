// ============================================================================
// Flipkart.com 1,00,000 Unique Products Bulk Direct-HTTP Harvester
// ============================================================================
// Uses direct HTTP requests to Flipkart's SSR search pages (`GET /search?...`,
// 40 structured JSON products per page via `window.__INITIAL_STATE__`) to
// harvest 100,000 unique Flipkart PIDs across 50 popular categories (2,000 per
// category) and record productive `searchPages` for fast 5-minute monitoring.
// ============================================================================

const fs = require("fs");
const path = require("path");
const { fetchSearchPageDirectHttp } = require("./fetch-worker");
const { compactProductRecord } = require("./price-monitor");

const DEFAULT_TARGET_PER_CATEGORY = 2000;
const CONCURRENCY = parseInt(process.env.HARVEST_CONCURRENCY || "16", 10);
const MIN_DISCOUNT_PCT = 50;

const DISCOUNT_50_FACET =
  "p%5B%5D=facets.discount_range_v1%255B%255D%3D50%2525%2Bor%2Bmore";

const SORT_ORDERS = [
  "relevance",
  "popularity",
  "price_asc",
  "price_desc",
  "recency_desc",
];

// 50 High-Volume Flipkart Categories × 2,000 SKUs = 1,00,000 Unique Products
const CATEGORIES = [
  {
    name: "smartphones",
    target: 2000,
    queries: [
      "5g smartphone",
      "samsung galaxy mobile phone",
      "redmi xiaomi 5g mobile",
      "realme 5g smartphone",
      "motorola 5g smartphone",
      "vivo 5g mobile phone",
      "oppo 5g smartphone",
      "poco 5g gaming phone",
      "iqoo oneplus 5g mobile",
      "apple iphone mobile",
      "nothing cmf pixel smartphone",
      "infinix tecno lava 5g mobile",
      "8gb ram 256gb smartphone",
      "amoled display 5g phone",
      "keypad feature mobile phone",
      "refurbished mobile smartphone",
      "gaming smartphone 12gb ram",
      "budget android mobile phone",
    ],
  },
  {
    name: "laptops",
    target: 2000,
    queries: [
      "laptop",
      "gaming laptop rtx",
      "hp core i5 i7 laptop",
      "dell inspiron latitude laptop",
      "lenovo ideapad thinkpad loq laptop",
      "asus vivobook tuf rog laptop",
      "acer aspire nitro swift laptop",
      "msi gaming thin light laptop",
      "apple macbook air pro",
      "samsung galaxy book laptop",
      "amd ryzen 5 ryzen 7 laptop",
      "intel core ultra i3 laptop",
      "16gb ram 512gb ssd laptop",
      "chromebook student budget laptop",
      "2 in 1 touchscreen convertible laptop",
      "business professional thin laptop",
      "creator oled display laptop",
      "infinix zebronics wings laptop",
    ],
  },
  {
    name: "tablets & monitors",
    target: 2000,
    queries: [
      "android tablet 5g wifi",
      "samsung galaxy tab tablet",
      "lenovo xiaomi redmi pad tablet",
      "apple ipad wifi cellular",
      "oneplus realme honor pad tablet",
      "kids drawing writing tablet",
      "24 inch 27 inch ips monitor",
      "gaming monitor 144hz 180hz",
      "curved 32 inch 4k monitor",
      "lg samsung dell benq monitor",
      "acer msi viewsonic aoc monitor",
      "portable usb c display monitor",
      "ultrawide qhd 2k monitor",
      "graphics drawing pen tablet",
      "touchscreen monitor office",
      "cctv led hd monitor 19 inch",
      "all in one desktop pc computer",
      "mini pc cpu desktop computer",
    ],
  },
  {
    name: "televisions",
    target: 2000,
    queries: [
      "4k ultra hd smart led tv",
      "32 inch hd ready smart tv",
      "43 inch 4k google smart tv",
      "55 inch qled 4k smart tv",
      "65 inch 75 inch smart tv",
      "samsung lg sony smart tv",
      "xiaomi mi redmi smart tv",
      "tcl hisense vu acer smart tv",
      "thomson kodak blaupunkt smart tv",
      "motorola realme infinix smart tv",
      "coocaa sansui haier smart tv",
      "dolby vision atmos qled tv",
      "oled mini led 4k television",
      "frameless android led tv",
      "40 inch full hd smart tv",
      "50 inch ultra hd smart tv",
      "smart projector 4k home theater",
      "portable mini led android projector",
    ],
  },
  {
    name: "headphones & earbuds",
    target: 2000,
    queries: [
      "tws true wireless bluetooth earbuds",
      "anc active noise cancelling earbuds",
      "boat airdopes bluetooth earbuds",
      "oneplus realme oppo wireless earbuds",
      "boult noise mivi truke earbuds",
      "jbl sony samsung wireless earbuds",
      "bluetooth neckband wireless earphones",
      "wireless over ear bluetooth headphones",
      "wired gaming headset with mic",
      "sony jbl sennheiser marshall headphones",
      "boat zebronics rockerz headphones",
      "wired in ear type c earphones",
      "gaming low latency rgb earbuds",
      "studio monitor dj headphones",
      "workout sports waterproof earphones",
      "kids safe volume headphones",
      "bone conduction open ear headphones",
      "bluetooth single ear calling headset",
    ],
  },
  {
    name: "speakers & soundbars",
    target: 2000,
    queries: [
      "portable bluetooth speaker waterproof",
      "party trolley karaoke speaker mic",
      "dolby atmos soundbar subwoofer",
      "home theater 5.1 2.1 speaker system",
      "jbl sony bose marshall speaker",
      "boat zebronics mivi portronics speaker",
      "philips lg samsung govo soundbar",
      "tower floor standing speaker",
      "computer pc desktop usb speakers",
      "bookshelf studio monitor speakers",
      "mini pocket bluetooth speaker",
      "smart speaker alexa echo",
      "fm radio retro bluetooth speaker",
      "car coaxial component speakers",
      "subwoofer amplifier bass tube",
      "megaphone pa amplifier speaker",
      "wireless karaoke mic speaker",
      "rgb light portable bluetooth speaker",
    ],
  },
  {
    name: "smartwatches",
    target: 2000,
    queries: [
      "amoled bluetooth calling smartwatch",
      "noise fireboltt boat smartwatch",
      "fastrack titan beatxp smartwatch",
      "samsung galaxy apple smartwatch",
      "amazfit garmin oneplus smartwatch",
      "boult zebronics gizmore smartwatch",
      "round dial metal strap smartwatch",
      "gps outdoor rugged military smartwatch",
      "women luxury slim gold smartwatch",
      "kids 4g gps sim calling smartwatch",
      "fitness tracker smart band spo2",
      "smart health ring sleep tracker",
      "always on display curved smartwatch",
      "android 4g sim playstore smartwatch",
      "smartwatch silicone leather metal straps",
      "sports running waterproof smartwatch",
      "luxury stainless steel smart watch",
      "square dial bluetooth calling watch",
    ],
  },
  {
    name: "watches",
    target: 2000,
    queries: [
      "men analog stainless steel watch",
      "men chronograph leather strap watch",
      "men digital sports waterproof watch",
      "titan fastrack sonata casio watch",
      "fossil timex tommy hilfiger watch",
      "seiko citizen tissot automatic watch",
      "women analog rose gold bracelet watch",
      "women luxury designer stone watch",
      "couple combo analog watch gift set",
      "men mechanical skeleton automatic watch",
      "kids cartoon digital analog watch",
      "peter england allen solly van heusen watch",
      "french connection guess armani watch",
      "casio g shock edifice vintage watch",
      "men slim minimalist black dial watch",
      "women ceramic mesh strap watch",
      "wall clock silent decorative modern",
      "alarm table digital led clock",
    ],
  },
  {
    name: "cameras & accessories",
    target: 2000,
    queries: [
      "dslr mirrorless digital camera",
      "4k action camera waterproof sports",
      "canon sony nikon fujifilm camera",
      "gopro insta360 dji action camera",
      "wifi 360 smart cctv security camera",
      "outdoor waterproof ptz cctv camera",
      "cp plus hikvision tapo qubo camera",
      "car dash cam dual channel 4k",
      "webcam 1080p 4k pc streaming",
      "instant photo polaroid kids camera",
      "smartphone dslr 3 axis gimbal stabilizer",
      "camera tripod aluminum ring light stand",
      "wireless collar lavalier microphone",
      "camera lens 50mm zoom prime filter",
      "studio softbox cob video led light",
      "dslr camera waterproof backpack bag",
      "dji drone camera foldable quadcopter",
      "binoculars monocular telescope zoom",
    ],
  },
  {
    name: "computer peripherals & storage",
    target: 2000,
    queries: [
      "mechanical rgb gaming keyboard",
      "wireless keyboard and mouse combo",
      "wireless ergonomic gaming mouse",
      "logitech hp dell zebronics keyboard mouse",
      "cosmic byte redragon ant esports gaming",
      "internal nvme m.2 sata ssd 1tb",
      "external portable ssd hard disk 1tb 2tb",
      "sandisk samsung wd seagate crucial storage",
      "ddr4 ddr5 laptop desktop ram 8gb 16gb",
      "pendrive usb 3.2 type c 128gb 256gb",
      "micro sd memory card 128gb 256gb",
      "wifi 6 dual band gigabit router mesh",
      "tp link d link tenda netgear router",
      "all in one ink tank laser printer",
      "hp canon epson brother printer",
      "graphics card rtx motherboard cpu cabinet",
      "computer ups 600va 1000va power supply",
      "usb c hub hdmi cable laptop cooling stand",
    ],
  },
  {
    name: "mobile accessories",
    target: 2000,
    queries: [
      "iphone 16 15 14 magsafe back cover",
      "samsung galaxy s24 s25 back cover case",
      "oneplus redmi realme vivo back cover",
      "motorola oppo poco shockproof mobile cover",
      "tempered glass screen guard privacy",
      "camera lens protector metal ring",
      "bluetooth selfie stick tripod with light",
      "car dashboard magnetic phone holder",
      "bike waterproof mobile mount charger",
      "desktop adjustable aluminum phone stand",
      "mobile gaming cooler trigger sleeves",
      "spigen ringke dailyobjects mobile cover",
      "waterproof underwater mobile pouch",
      "magsafe magnetic wallet pop socket grip",
      "otg adapter usb c audio splitter",
      "smartwatch charging dock silicone strap",
      "airpods earbuds silicone hard cover case",
      "ipad tablet flip leather keyboard cover",
    ],
  },
  {
    name: "power banks & chargers",
    target: 2000,
    queries: [
      "20000mah fast charging power bank",
      "10000mah slim pocket power bank",
      "magsafe magnetic wireless power bank",
      "mi redmi ambrane urbn boat power bank",
      "anker stuffcool portronics spigen power bank",
      "65w 100w laptop charging power bank",
      "gan 65w 45w 33w fast wall charger",
      "20w pd type c charger adapter iphone",
      "samsung original 25w 45w fast charger",
      "braided usb type c 100w fast cable",
      "lightning micro usb fast charging cable",
      "3 in 1 wireless magsafe charging station",
      "car fast charger dual port pd 65w",
      "extension board surge protector usb",
      "multiport desktop charging station gan",
      "aa aaa rechargeable battery charger",
      "solar waterproof camping power bank",
      "laptop charger adapter 65w 90w",
    ],
  },
  {
    name: "refrigerators & washing machines",
    target: 2000,
    queries: [
      "double door frost free refrigerator",
      "single door direct cool refrigerator",
      "side by side convertible refrigerator",
      "samsung lg whirlpool refrigerator",
      "haier godrej bosch ifb refrigerator",
      "mini portable car fridge deep freezer",
      "front load fully automatic washing machine",
      "top load fully automatic washing machine",
      "semi automatic washing machine 7kg 8kg",
      "lg samsung bosch ifb washing machine",
      "whirlpool haier godrej marq washing machine",
      "washer dryer combo inbuilt heater",
      "portable mini foldable washing machine",
      "dishwasher 12 14 place countertop bosch",
      "washing machine stand cover inlet pipe",
      "refrigerator stabilizer stand mat cover",
      "triple door bottom mount fridge",
      "9kg 10kg heavy duty washing machine",
    ],
  },
  {
    name: "air conditioners & coolers",
    target: 2000,
    queries: [
      "1.5 ton 5 star inverter split ac",
      "1.5 ton 3 star inverter split ac",
      "1 ton 2 ton split air conditioner",
      "window air conditioner 1.5 ton",
      "lg daikin voltas carrier split ac",
      "bluestar panasonic lloyd haier ac",
      "godrej hitachi marq whirlpool ac",
      "hot and cold inverter split ac",
      "desert air cooler 50l 70l 90l",
      "personal room air cooler honeycomb",
      "symphony bajaj crompton orient air cooler",
      "kenstar hindware havells air cooler",
      "tower air cooler slim portable",
      "ac voltage stabilizer 4kva 5kva copper",
      "oil filled radiator room heater",
      "fan blower quartz halogen room heater",
      "bajaj orient havells usha room heater",
      "ac outdoor cover stand cleaning kit",
    ],
  },
  {
    name: "kitchen appliances",
    target: 2000,
    queries: [
      "750w 1000w mixer grinder juicer",
      "prestige bajaj philips butterfly mixer",
      "sujata preethi bosch crompton mixer",
      "digital air fryer basket oven home",
      "philips agaro pigeon inalsa air fryer",
      "convection solo grill microwave oven",
      "otg oven toaster griller 30l 40l",
      "induction cooktop 2000w touch panel",
      "3 burner 4 burner glass gas stove",
      "auto clean kitchen chimney 60cm 90cm",
      "electric kettle stainless steel 1.5l",
      "sandwich grill toaster waffle maker",
      "espresso drip coffee maker milk frother",
      "hand blender electric chopper beater",
      "cold press slow juicer nutri blender",
      "electric rice cooker egg boiler roti maker",
      "wet grinder food processor atta maker",
      "ro uv uf copper alkaline water purifier",
    ],
  },
  {
    name: "home appliances & fans",
    target: 2000,
    queries: [
      "bldc energy saving ceiling fan remote",
      "1200mm high speed decorative ceiling fan",
      "atomberg crompton havells orient fan",
      "bajaj usha polycab luminous ceiling fan",
      "pedestal stand wall mounted table fan",
      "exhaust ventilation fan kitchen bathroom",
      "15l 25l storage water heater geyser",
      "instant 3l 5l gas water heater geyser",
      "bajaj crompton havells ao smith geyser",
      "racold v guard orient immersion rod",
      "steam dry iron garment steamer clothes",
      "philips bajaj morphy richards usha iron",
      "electric automatic sewing silai machine",
      "usha singer brother sewing machine",
      "home inverter pure sine wave battery",
      "luminous microtek exide amaron inverter",
      "solar panel hybrid inverter stabilizer",
      "mosquito killer racket uv lamp trap",
    ],
  },
  {
    name: "vacuum cleaners & purifiers",
    target: 2000,
    queries: [
      "wet and dry vacuum cleaner home",
      "cordless stick handheld vacuum cleaner",
      "robotic vacuum cleaner and mop lidar",
      "eureka forbes agaro philips vacuum",
      "dyson karcher dyson dreame vacuum",
      "inalsa black decker bosch kent vacuum",
      "car handheld portable vacuum cleaner",
      "sofa carpet mattress uv vacuum cleaner",
      "high pressure car washer pump machine",
      "steam mop floor sanitizer cleaner",
      "hepa h13 air purifier for home",
      "philips dyson coway mi honeywell purifier",
      "car ionizer mini air purifier",
      "ultrasonic cool mist room humidifier",
      "room dehumidifier moisture absorber",
      "water purifier filter cartridge kit",
      "aquaguard kent pureit livpure purifier",
      "industrial heavy duty blower vacuum",
    ],
  },
  {
    name: "sofas & recliners",
    target: 2000,
    queries: [
      "3 seater fabric sofa living room",
      "l shape sectional corner sofa set",
      "5 seater 6 seater wooden sofa set",
      "2 seater compact loveseat sofa",
      "leatherette chesterfield velvet sofa",
      "sofa cum bed folding wooden fabric",
      "single seater manual motorized recliner",
      "2 seater 3 seater recliner sofa",
      "rocking massage home theater recliner",
      "xxl xxxl bean bag filled with beans",
      "wooden accent armchair lounge chair",
      "ottoman pouffe footrest storage stool",
      "wakefit sleepyhead nilkamal sofa",
      "bharat lifestyle adorno wooden sofa",
      "sheesham solid wood sofa living room",
      "inflatable folding sofa bed lounge",
      "outdoor patio balcony sofa chair set",
      "hanging swing hammock jhula chair",
    ],
  },
  {
    name: "beds & mattresses",
    target: 2000,
    queries: [
      "king size engineered wood bed storage",
      "queen size solid sheesham wood bed",
      "hydraulic box storage double bed",
      "single metal wooden bed bedroom",
      "folding rollaway metal bed with mattress",
      "bunk bed for kids hostel bedroom",
      "upholstered cushioned headboard bed",
      "orthopedic memory foam mattress king",
      "queen size pocket spring mattress",
      "single bed foam mattress 6 inch",
      "wakefit sleepyhead sleepwell mattress",
      "kurlon duroflex nilkamal peps mattress",
      "natural latex dual comfort mattress",
      "foldable travel floor gadda mattress",
      "memory foam mattress topper protector",
      "bedside table wooden nightstand drawer",
      "wooden headboard canopy poster bed",
      "baby wooden crib cot bed mosquito net",
    ],
  },
  {
    name: "wardrobes & tables",
    target: 2000,
    queries: [
      "2 door 3 door 4 door wooden wardrobe",
      "sliding door engineered wood wardrobe",
      "steel metal almirah godrej locker",
      "foldable portable cloth cupboard almirah",
      "4 seater 6 seater wooden dining table",
      "marble glass top dining table set",
      "folding space saving dining table",
      "coffee center table living room teapoy",
      "marble round nesting coffee table",
      "wooden tv entertainment unit cabinet",
      "wall mounted floating tv unit stand",
      "wooden dressing table mirror storage",
      "vanity makeup table stool led mirror",
      "wooden shoe rack cabinet with door",
      "wooden bookshelf bookcase display rack",
      "chest of drawers bedroom dresser",
      "kitchen crockery cabinet sideboard",
      "temple wooden pooja mandir home",
    ],
  },
  {
    name: "office chairs & study desks",
    target: 2000,
    queries: [
      "ergonomic high back mesh office chair",
      "executive leatherette boss revolving chair",
      "racing rgb gaming chair footrest",
      "study desk work from home chair",
      "lumbar support adjustable office chair",
      "green soul wakefit cellbell office chair",
      "nilkamal featherlite da urban chair",
      "plastic dining café visitor chairs set",
      "wooden study computer table drawers",
      "l shaped corner office workstation desk",
      "electric height adjustable standing desk",
      "gaming computer table rgb cable desk",
      "wall mounted folding study laptop table",
      "foldable bed laptop table cup holder",
      "solid sheesham wood writing office desk",
      "bar stool high kitchen counter chair",
      "folding study chair writing pad",
      "monitor arm desk mount laptop riser",
    ],
  },
  {
    name: "cookware & dinnerware",
    target: 2000,
    queries: [
      "non stick cookware set kadai frying pan",
      "triply stainless steel kadai saucepan",
      "cast iron dosa tawa skillet kadai",
      "pressure cooker 3l 5l stainless hard anodized",
      "prestige hawkins pigeon wonderchef cookware",
      "vinod meyer bergner stahl cookware",
      "opalware bone china dinner set 35 pcs",
      "melamine stainless steel dinner thali set",
      "cello la opala borosil larah dinner set",
      "ceramic plates bowls coffee mug set",
      "insulated casserole hot pot set of 3",
      "idli maker steamer appam patra tadka pan",
      "glass mixing bowls microwave safe set",
      "tea cup saucer kettle serving tray set",
      "stainless steel cutlery spoon fork set",
      "brass copper kansa dinner thali jug",
      "wine whiskey cocktail water glass set",
      "non stick cake baking tray mould set",
    ],
  },
  {
    name: "kitchen storage & tools",
    target: 2000,
    queries: [
      "airtight kitchen storage containers set",
      "borosilicate glass jars spice masala dabba",
      "insulated stainless steel lunch box tiffin",
      "thermosteel vacuum water bottle 1 litre",
      "copper water bottle fridge bottle set",
      "milton cello borosil signoraware bottle",
      "oil dispenser glass steel leakproof",
      "fridge vegetable storage organizer boxes",
      "stainless steel dish drying rack basket",
      "kitchen trolley 4 tier fruit vegetable rack",
      "chef knife set wooden chopping board",
      "manual vegetable chopper slicer grater",
      "silicone cooking spatula ladle utensil set",
      "wooden marble chakla belan rolling pin",
      "mortar pestle digital kitchen scale",
      "rotating spice rack cutlery holder",
      "tea coffee sugar canister jar set",
      "grain atta storage drum steel plastic",
    ],
  },
  {
    name: "bedsheets & bedding",
    target: 2000,
    queries: [
      "100% cotton king size double bedsheet",
      "fitted elastic king queen bedsheet",
      "jaipuri rajasthani floral printed bedsheet",
      "300 tc 400 tc satin silk luxury bedsheet",
      "bombay dyeing trident spaces bedsheet",
      "story@home cortina divine casa bedsheet",
      "single bed cotton bedsheet pillow cover",
      "reversible ac comforter blanket double",
      "heavy winter mink woollen blanket double",
      "cotton reversible ac dohar rajai quilt",
      "orthopedic memory foam cervical pillow",
      "microfiber sleeping pillows set of 2",
      "decorative cushion covers 16x16 velvet",
      "cushion fillers inserts bolster pillow",
      "diwan set 8 pieces sofa slipcover",
      "waterproof terry cotton mattress protector",
      "foldable king size double bed mosquito net",
      "knitted boho sofa throw travel neck pillow",
    ],
  },
  {
    name: "curtains carpets & bath linen",
    target: 2000,
    queries: [
      "blackout window door curtains 7ft 9ft",
      "sheer net velvet floral eyelet curtains",
      "roller zebra blinds pvc balcony curtain",
      "shaggy fluffy area carpet rug living room",
      "persian woven cotton jute floor rug",
      "anti skid hallway runner doormat set",
      "artificial grass turf mat balcony",
      "kids reversible foam play mat",
      "100% cotton 500gsm 600gsm bath towel",
      "bamboo microfiber quick dry bath towel",
      "hand face towel set kids hooded towel",
      "terry cotton bathrobe men women",
      "memory foam diatomite bath floor mat",
      "waterproof shower curtain soap dispenser",
      "trident spaces bombay dyeing towel",
      "rain shower head stainless bathroom tap",
      "bathroom corner shelf towel rack caddy",
      "table cloth runner dining place mats",
    ],
  },
  {
    name: "home decor & wall art",
    target: 2000,
    queries: [
      "living room home decor showpiece statue",
      "ceramic nordic donut flower vase",
      "framed canvas wall painting art set",
      "metal wall art hanging living room",
      "wooden floating wall shelves organizer",
      "led backlit mirror full length floor mirror",
      "brass antique buddha ganesha idol statue",
      "indoor tabletop water fountain decor",
      "artificial indoor plant with ceramic pot",
      "collage photo frames set wall decor",
      "macrame boho wall hanging wind chime",
      "candle holder urli bowl lantern tealight",
      "scented aromatic candle reed diffuser",
      "key holder wooden wall hanging decor",
      "3d wallpaper self adhesive wall stickers",
      "resin deer horse abstract modern sculpture",
      "ceramic indoor planter pot metal stand",
      "festive diwali toran bandarwal decor",
    ],
  },
  {
    name: "lighting & lamps",
    target: 2000,
    queries: [
      "crystal modern chandelier ceiling jhumar",
      "hanging pendant light dining table lamp",
      "bedside table ceramic wooden night lamp",
      "standing corner floor lamp living room",
      "rechargeable led study desk reading lamp",
      "wall sconce antique bedroom lamp",
      "led bulb batten tube panel ceiling light",
      "philips wipro syska havells orient led",
      "smart wifi rgb alexa led bulb strip",
      "fairy copper string curtain festive light",
      "3d moon galaxy crystal night lamp",
      "motion sensor rechargeable cabinet light",
      "solar waterproof garden pathway gate light",
      "rechargeable emergency inverter led light",
      "cob spotlight track light downlight",
      "neon sign led wall gaming decor light",
      "rgb sunset projector ambient lamp",
      "himalayan salt lamp vintage edison bulb",
    ],
  },
  {
    name: "home organization & storage",
    target: 2000,
    queries: [
      "wardrobe foldable cloth organizer bag",
      "underbed blanket saree storage box",
      "foldable laundry basket hamper lid",
      "velvet wooden plastic clothes hangers",
      "modular plastic drawer storage chest",
      "acrylic makeup cosmetic vanity box",
      "vacuum compression storage bags clothes",
      "clear stackable shoe storage boxes",
      "stainless steel foldable cloth drying stand",
      "folding ironing board heavy duty stand",
      "pedal sensor kitchen bathroom dustbin",
      "jewellery watch sunglasses organizer box",
      "rolling 3 tier metal utility cart",
      "over the door wall adhesive hooks rack",
      "spin mop bucket floor cleaning wiper",
      "broom mop holder rack storage cabinet",
      "cable wire medicine first aid organizer",
      "garden plant pots watering hose tools",
    ],
  },
  {
    name: "power & mechanical tools",
    target: 2000,
    queries: [
      "impact electric drill machine 13mm kit",
      "cordless lithium screwdriver drill 12v 20v",
      "rotary hammer demolition drill machine",
      "bosch black decker stanley dewalt drill",
      "ibell ingco cheetah makita power tools",
      "angle grinder marble wood circular saw",
      "socket wrench ratchet spanner tool kit",
      "combination spanner plier screwdriver set",
      "taparia stanley bosch hand tool kit",
      "complete home repair tool kit box",
      "precision magnetic screwdriver bit set",
      "electric hot air heat gun soldering iron",
      "digital multimeter clamp meter tester",
      "laser distance meter measuring tape level",
      "inverter arc welding machine paint sprayer",
      "hydraulic car jack torque wrench",
      "heavy duty metal tool box organizer",
      "smart digital fingerprint door lock safe",
    ],
  },
  {
    name: "car & bike accessories",
    target: 2000,
    queries: [
      "digital car tyre inflator air compressor",
      "android car stereo touchscreen player",
      "7d custom fit car floor mats",
      "waterproof car bike body cover uv",
      "led headlight bulb h4 h7 fog light",
      "car seat neck pillow lumbar cushion",
      "car dashboard solar perfume freshener",
      "microfiber 800gsm car wash cloth shampoo",
      "car ceramic coating polish wax kit",
      "magnetic car window sunshade cover",
      "car trunk boot foldable organizer",
      "gps tracker obd2 scanner car alarm",
      "car jump starter battery charger",
      "bike chain lube cleaner brush kit",
      "motorcycle gel seat cushion tank bag",
      "bike disc lock anti theft alarm",
      "tubeless tyre puncture repair tool kit",
      "3m motul wavex grandpitstop auto care",
    ],
  },
  {
    name: "helmets & riding gear",
    target: 2000,
    queries: [
      "full face isi dot ece bike helmet",
      "flip up modular motorcycle helmet",
      "open face half scooter helmet men women",
      "studds steelbird vega bike helmet",
      "axor smk mt ls2 ignyte helmet",
      "royal enfield kyt riding helmet",
      "motorcycle riding jacket armor waterproof",
      "biker riding gloves knuckle protection",
      "knee elbow guard riding gear armor",
      "motorcycle waterproof rain suit gear",
      "bike saddle tail magnetic tank bag",
      "helmet bluetooth intercom headset",
      "anti fog visor pinlock helmet balaclava",
      "riding boots ankle protection motorcycle",
      "kids bicycle skating helmet safety",
      "bike led indicator hazard auxiliary light",
      "motorcycle handlebar grip mirror levers",
      "helmet lock net bungee cargo cord",
    ],
  },
  {
    name: "mens tshirts & polos",
    target: 2000,
    queries: [
      "men cotton round neck t shirt",
      "men polo collar t shirt cotton",
      "men oversized drop shoulder t shirt",
      "men graphic printed streetwear t shirt",
      "men gym sports dry fit t shirt",
      "men full sleeve henley t shirt",
      "men pack of 3 5 cotton t shirts",
      "us polo assn allen solly men t shirt",
      "van heusen peter england men polo",
      "levis pepe jeans jack jones men t shirt",
      "puma adidas nike reebok men t shirt",
      "hrx wrogn roadster highlander men t shirt",
      "the souled store bewakoof oversized t shirt",
      "metronaut provoke campus sutra men t shirt",
      "men striped colorblock polo t shirt",
      "men v neck solid cotton t shirt",
      "men textured waffle knit t shirt",
      "men compression quick dry running tee",
    ],
  },
  {
    name: "mens shirts",
    target: 2000,
    queries: [
      "men formal cotton slim fit shirt",
      "men casual checkered plaid cotton shirt",
      "men pure linen solid summer shirt",
      "men denim button down casual shirt",
      "men floral tropical beach resort shirt",
      "men mandarin chinese collar shirt",
      "men overshirt flannel corduroy shacket",
      "men satin solid party wear shirt",
      "allen solly van heusen men formal shirt",
      "louis philippe peter england men shirt",
      "arrow raymond park avenue men shirt",
      "us polo assn levis wrogn men shirt",
      "roadster highlander dennis lingo men shirt",
      "indian garage co snitch men shirt",
      "men half sleeve cuban camp collar shirt",
      "men oxford button down solid shirt",
      "men textured popcorn knit casual shirt",
      "men striped formal office cotton shirt",
    ],
  },
  {
    name: "mens jeans & trousers",
    target: 2000,
    queries: [
      "men slim fit stretch denim jeans",
      "men regular straight fit blue jeans",
      "men relaxed baggy loose fit jeans",
      "men tapered black distressed jeans",
      "levis wrangler pepe jeans men jeans",
      "spykar mufti killer jack jones men jeans",
      "flying machine roadster highlander jeans",
      "men slim fit cotton chinos trousers",
      "men formal pleated office trousers",
      "men multi pocket cargo cotton pants",
      "men cotton joggers cuffed track pants",
      "men gym running sports track pants",
      "men athletic cotton denim shorts",
      "allen solly van heusen men trousers",
      "peter england louis philippe men chinos",
      "raymond arrow park avenue men trousers",
      "puma adidas nike hrx men track pants",
      "men linen drawstring relaxed trousers",
    ],
  },
  {
    name: "mens ethnic & jackets",
    target: 2000,
    queries: [
      "men silk blend kurta pyjama set",
      "men cotton chikankari pathani kurta",
      "men nehru modi waistcoat jacket kurta",
      "men wedding sherwani indo western set",
      "manyavar sojanya vastramay men kurta",
      "men formal 2 piece suit blazer",
      "men slim fit casual party blazer",
      "men winter puffer quilted bomber jacket",
      "men blue black denim trucker jacket",
      "men faux leather biker moto jacket",
      "men fleece pullover zipper hoodie",
      "men cotton graphic sweatshirt winter",
      "men woollen pullover cardigan sweater",
      "men waterproof windcheater rain jacket",
      "puma adidas nike men hoodie jacket",
      "allen solly us polo woodland men jacket",
      "levis jack jones campus sutra men jacket",
      "men varsity baseball sleeveless vest",
    ],
  },
  {
    name: "sarees",
    target: 2000,
    queries: [
      "banarasi pure silk wedding saree",
      "kanjivaram silk traditional festive saree",
      "cotton printed daily wear saree",
      "georgette party wear sequin saree",
      "organza floral embroidered saree",
      "chiffon lightweight summer saree",
      "bandhani rajasthani patola silk saree",
      "chanderi paithani mysore silk saree",
      "sambalpuri tant kota doria cotton saree",
      "linen zari border handloom saree",
      "net embroidered bridal reception saree",
      "satin silk plain solid party saree",
      "ready to wear pre draped saree",
      "chikankari kalamkari tussar silk saree",
      "mirror work velvet border wedding saree",
      "siril anand sarees mirchi fashion saree",
      "mimosa akhilam kanchipuram silk saree",
      "readymade stitched designer saree blouse",
    ],
  },
  {
    name: "kurtis & kurta sets",
    target: 2000,
    queries: [
      "women cotton straight printed kurti",
      "women anarkali flared kurta dupatta set",
      "women embroidered kurta palazzo set",
      "women chikankari lucknowi rayon kurti",
      "women festive silk kurta pant dupatta",
      "women alia cut nyra cut kurta set",
      "women sharara garara wedding suit set",
      "women short kurti top for jeans",
      "women bandhani jaipuri cotton kurta set",
      "women stitched lehenga choli bridal",
      "biba w for woman aurelia kurta set",
      "libas janasya indo era kurta set",
      "gosriki rangita anouk sangria kurta",
      "varanga vbuyz festive embroidered kurta",
      "women velvet woolen winter kurti set",
      "women muslin chanderi silk kurta set",
      "women pakistani heavy embroidered suit",
      "women cotton dupatta ethnic palazzo",
    ],
  },
  {
    name: "womens dresses & western",
    target: 2000,
    queries: [
      "women floral maxi long summer dress",
      "women midi a line casual party dress",
      "women bodycon cocktail evening dress",
      "women fit and flare skater dress",
      "women satin slip sequin party dress",
      "women wrap shirt belted knee dress",
      "women ethnic fusion anarkali maxi dress",
      "women western stylish co ord set",
      "women jumpsuit playsuit party wear",
      "women formal office blazer coat suit",
      "women winter puffer trench coat jacket",
      "women woollen cardigan shrug sweater",
      "women oversized hoodie fleece sweatshirt",
      "women denim cropped leather biker jacket",
      "vero moda only mango forever 21 dress",
      "tokyo talkies harpa sassafras dress",
      "marks spencer allen solly women dress",
      "berrylush rare kazo madame women dress",
    ],
  },
  {
    name: "womens jeans & tops",
    target: 2000,
    queries: [
      "women high waist wide leg denim jeans",
      "women straight mom boyfriend baggy jeans",
      "women skinny flared bootcut denim jeans",
      "levis only vero moda women jeans",
      "kraus tokyo talkies kotty women jeans",
      "women casual floral printed cotton top",
      "women formal office shirt blouse top",
      "women crop top western party wear",
      "women peplum ruffle chiffon tunic top",
      "women oversized cotton t shirt graphic",
      "women ribbed fitted basic tank top",
      "women high waist wide leg formal trousers",
      "women korean pleated cargo utility pants",
      "women cotton joggers gym yoga leggings",
      "women flared palazzo cigarette pants",
      "women denim mini midi pleated skirt",
      "harpa miss chase sassafras women top",
      "marks spencer allen solly women trousers",
    ],
  },
  {
    name: "innerwear & loungewear",
    target: 2000,
    queries: [
      "women cotton printed night suit pajama",
      "women long cotton maxi nighty gown",
      "women padded high impact sports bra",
      "women seamless t shirt everyday bra",
      "women cotton hipster bikini briefs pack",
      "women tummy tucker shapewear camisole",
      "women satin bridal robe loungewear set",
      "jockey enamor clovia zivame women bra",
      "van heusen marks spencer women innerwear",
      "men cotton trunks boxer briefs pack",
      "men woven cotton relaxed boxer shorts",
      "men combed cotton vests undershirts",
      "men cotton lounge pajama pants shorts",
      "men winter thermal innerwear warmer set",
      "jockey damensch xyxx men trunks vests",
      "van heusen us polo levis men innerwear",
      "men women athletic ankle crew socks",
      "women winter fleece warm nightwear set",
    ],
  },
  {
    name: "kids clothing",
    target: 2000,
    queries: [
      "boys cotton t shirt shorts combo pack",
      "boys denim jeans cargo joggers pants",
      "boys party wear blazer suit shirt set",
      "boys ethnic silk kurta pyjama set",
      "boys winter puffer jacket hoodie sweater",
      "girls party wear net gown frock dress",
      "girls cotton summer casual frock dress",
      "girls ethnic lehenga choli festive set",
      "girls kurta palazzo sharara ethnic set",
      "girls t shirt leggings jeans coord set",
      "girls winter cardigan jacket sweater",
      "baby boy girl newborn romper onesie set",
      "kids cotton night suit pajama set",
      "hopscotch max kids boys girls clothing",
      "allen solly junior us polo kids clothing",
      "marks spencer pantaloons junior kids",
      "babyhug gini jony kids dresses jeans",
      "kids school uniform raincoat swimwear",
    ],
  },
  {
    name: "mens footwear",
    target: 2000,
    queries: [
      "men running sports gym training shoes",
      "men casual chunky white fashion sneakers",
      "men genuine leather formal oxford derby",
      "men slip on driving penny loafers",
      "men chelsea ankle trekking leather boots",
      "men casual leather velcro sports sandals",
      "men orthopedic soft flip flops slippers",
      "men waterproof garden clogs sliders",
      "men ethnic wedding jutti mojari",
      "nike adidas puma reebok men shoes",
      "skechers asics new balance men shoes",
      "campus sparx asian bruton men shoes",
      "red tape woodland bata hush puppies men",
      "us polo assn clarks lee cooper men shoes",
      "crocs birkenstock neeman bacca bucci men",
      "men badminton tennis court football shoes",
      "men steel toe industrial safety boots",
      "men canvas slip on walking sneakers",
    ],
  },
  {
    name: "womens footwear",
    target: 2000,
    queries: [
      "women block heels wedges party sandals",
      "women flat casual daily wear sandals",
      "women ethnic embroidered jutti mojari",
      "women kolhapuri leather flat chappal",
      "women formal ballerina bellies loafers",
      "women running walking memory foam shoes",
      "women casual white chunky sneakers",
      "women stiletto kitten heel party pumps",
      "women orthopedic soft doctor slippers",
      "women waterproof clogs sliders flip flops",
      "women ankle winter chelsea leather boots",
      "metro mochi bata catwalk women sandals",
      "inc 5 marc loire shoetopia women heels",
      "skechers puma adidas campus women shoes",
      "asian sparx crocs clarks women footwear",
      "allen solly van heusen women heels flats",
      "women bridal golden silver wedge heels",
      "women mules backless strappy sandals",
    ],
  },
  {
    name: "handbags wallets & clutches",
    target: 2000,
    queries: [
      "women stylish sling crossbody handbag",
      "women large tote shopper shoulder bag",
      "women bridal clutch evening potli purse",
      "women zip around leather wallet purse",
      "women structured satchel hobo handbag",
      "women combo set of 3 handbags",
      "women stylish mini casual backpack",
      "lavie caprese baggit women handbag",
      "zouk miraggio lino perros women bag",
      "exotic mammon fastrack women handbag",
      "aldo guess fossil da milano handbag",
      "men genuine leather bi fold wallet",
      "men rfid slim metal card holder wallet",
      "wildhorn urban forest titan men wallet",
      "tommy hilfiger allen solly men wallet",
      "van heusen hidesign leather belt wallet",
      "women cosmetic makeup travel pouch bag",
      "men chest sling waist fanny pack bag",
    ],
  },
  {
    name: "luggage backpacks & office bags",
    target: 2000,
    queries: [
      "hard sided cabin trolley bag 55cm",
      "medium large suitcase trolley 65cm 75cm",
      "set of 3 polycarbonate luggage trolley",
      "safari american tourister vip skybags",
      "aristocrat kamiliant mokobara uppercase",
      "laptop backpack 15.6 inch waterproof",
      "college school casual travel backpack",
      "trekking hiking rucksack 60l 70l",
      "wildcraft safari skybags gear backpack",
      "arctic hunter fur jaden tommy backpack",
      "men leather laptop office messenger bag",
      "women office tote laptop handbag",
      "executive briefcase expandable office bag",
      "travel duffle weekender wheeled bag",
      "packing cubes toiletry travel organizer",
      "passport holder luggage scale tsa lock",
      "laptop sleeve 15.6 inch document folder",
      "gym duffle sports shoe compartment bag",
    ],
  },
  {
    name: "sunglasses & jewellery",
    target: 2000,
    queries: [
      "polarized uv sunglasses men aviator",
      "oversized cat eye sunglasses women",
      "wayfarer square retro round sunglasses",
      "ray ban oakley fastrack vincent chase",
      "blue cut zero power computer glasses",
      "925 sterling silver necklace ring earrings",
      "american diamond cz choker jewellery set",
      "kundan pearl traditional bridal necklace",
      "gold plated temple haram mangalsutra",
      "oxidised silver boho jhumka bangles",
      "giva clara zavya sukhi fashion jewellery",
      "swarovski voylla yellow chimes jewellery",
      "women rose gold bracelet pendant anklets",
      "men sterling silver steel chain bracelet",
      "bridal matha patti maang tikka payal",
      "pearl drop stud hoop earrings women",
      "hair clips claws scrunchies bridal pins",
      "leather formal belt cufflinks tie combo",
    ],
  },
  {
    name: "perfumes & grooming",
    target: 2000,
    queries: [
      "eau de parfum luxury perfume men",
      "long lasting floral perfume women",
      "arabian oud perfume lattafa armaf ajmal",
      "davidoff calvin klein versace jaguar",
      "titan skinn bella vita villian wild stone",
      "perfume gift set attar non alcoholic",
      "beard trimmer cordless men grooming kit",
      "electric rotary foil shaver men",
      "philips braun panasonic mi beard trimmer",
      "havells nova vega syska beard trimmer",
      "ionic hair dryer 2000w professional",
      "ceramic hair straightener curler brush",
      "philips dyson ikonic agaro hair styler",
      "women epilator painless facial trimmer",
      "electric sonic toothbrush water flosser",
      "men 5 blade safety razor shaving kit",
      "deep tissue percussion massage gun",
      "electric leg foot neck back massager",
    ],
  },
  {
    name: "skincare makeup & haircare",
    target: 2000,
    queries: [
      "vitamin c niacinamide hyaluronic serum",
      "spf 50 pa++++ matte gel sunscreen",
      "minimalist derma co plum dot key",
      "cetaphil cerave neutrogena bioderma",
      "korean snail mucin retinol night cream",
      "matte liquid lipstick lip gloss balm",
      "full coverage foundation concealer compact",
      "eyeshadow palette eyeliner kajal mascara",
      "maybelline lakme loreal sugar cosmetics",
      "swiss beauty mars colorbar faces canada",
      "makeup brushes beauty blender vanity kit",
      "anti hair fall dandruff sulfate shampoo",
      "keratin hair conditioner mask serum",
      "rosemary onion bhringraj hair growth oil",
      "loreal tresemme dove mamaearth haircare",
      "body wash shower gel lotion scrub",
      "deodorant body mist roll on men women",
      "beard oil wash facial kit salon gold",
    ],
  },
  {
    name: "sports fitness & cycles",
    target: 2000,
    queries: [
      "motorized folding treadmill walking pad",
      "exercise cycle spin air bike home gym",
      "adjustable dumbbell barbell home gym set",
      "resistance bands skipping rope ab roller",
      "tpe non slip yoga mat gym gloves belt",
      "protein shaker kettlebell foam roller",
      "mountain cycle mtb 21 speed adult",
      "hybrid city kids electric e-bike cycle",
      "hero hercules firefox urban terrain cycle",
      "kashmir english willow cricket bat kit",
      "yonex li ning badminton racket shuttle",
      "football basketball volleyball cosco nivia",
      "boxing punching bag gloves roller skates",
      "camping tent sleeping bag trekking pole",
      "whey protein isolate powder 1kg 2kg",
      "optimum nutrition muscleblaze avvatar whey",
      "creatine mass gainer multivitamin omega 3",
      "blood pressure monitor oximeter nebulizer",
    ],
  },
  {
    name: "books toys stationery & baby care",
    target: 2000,
    queries: [
      "bestseller self help finance fiction book",
      "upsc jee neet ssc competitive exam books",
      "children story books encyclopedia manga",
      "lego magnetic blocks stem science toy",
      "remote control rc car drone robot toy",
      "soft plush teddy bear doll house kitchen",
      "board games puzzles chess carrom uno",
      "hot wheels nerf blaster diecast cars",
      "baby foldable stroller pram car seat",
      "baby walker high chair carrier crib",
      "baby diapers pants wet wipes feeding bottle",
      "pampers huggies mamypoko chicco luvlap",
      "hardcover notebook diary planner pen set",
      "acrylic watercolor paint sketchbook canvas",
      "camlin faber castell brustro parker art",
      "office paper shredder laminator whiteboard",
      "dog cat food bed collar pet supplies",
      "acoustic guitar electronic keyboard ukulele",
    ],
  },
];

function loadData(dataFile) {
  try {
    if (fs.existsSync(dataFile)) {
      return JSON.parse(fs.readFileSync(dataFile, "utf-8"));
    }
  } catch {}
  return { products: {}, lastRun: null, runCount: 0 };
}

function loadSkus(skuFile) {
  try {
    if (fs.existsSync(skuFile)) {
      const parsed = JSON.parse(fs.readFileSync(skuFile, "utf-8"));
      return {
        categories: parsed.categories || {},
        searchPages: parsed.searchPages || [],
        customSkus: parsed.customSkus || [],
        skus: parsed.skus || [],
      };
    }
  } catch {}
  return { categories: {}, searchPages: [], customSkus: [], skus: [] };
}

async function harvestCategory(
  cat,
  existingCategoryMap,
  globalProducts,
  usedGlobalPids,
  productiveSearchPages
) {
  const targetCount = cat.target || DEFAULT_TARGET_PER_CATEGORY;
  const collected = new Map();

  const existingList = existingCategoryMap[cat.name] || [];
  for (const pid of existingList) {
    const p = globalProducts[pid];
    if (p) {
      p.category = cat.name;
      collected.set(pid, p);
      usedGlobalPids.add(pid);
    }
  }

  if (collected.size >= targetCount) {
    console.log(
      `   ⏭️  [${cat.name.padEnd(32)}] Already has ${collected.size}/${targetCount} products`
    );
    return { skus: [...collected.keys()].slice(0, targetCount), added: 0 };
  }

  const startCount = collected.size;

  // Prioritized sub-waves: 50%+ discount facet first, then unfiltered across sorts & pages
  const subWaves = [];

  // Priority 1: 50%+ discount facet (pages 1..8 across relevance & popularity)
  for (const sort of SORT_ORDERS.slice(0, 3)) {
    const tasks = [];
    for (let pageNum = 1; pageNum <= 8; pageNum++) {
      for (const q of cat.queries) {
        tasks.push({
          uri: `/search?q=${encodeURIComponent(q)}&${DISCOUNT_50_FACET}&sort=${sort}&page=${pageNum}`,
          minDiscountPct: MIN_DISCOUNT_PCT,
        });
      }
    }
    subWaves.push({ label: `50%+:${sort}`, tasks });
  }

  // Priority 2: Unfiltered queries (any valid priced product — high yield, 40 SKUs/page)
  for (const sort of SORT_ORDERS) {
    const tasks = [];
    for (let pageNum = 1; pageNum <= 10; pageNum++) {
      for (const q of cat.queries) {
        tasks.push({
          uri: `/search?q=${encodeURIComponent(q)}&sort=${sort}&page=${pageNum}`,
          minDiscountPct: 0,
        });
      }
    }
    subWaves.push({ label: `any:${sort}`, tasks });
  }

  let totalQueriesRun = 0;
  let batchCounter = 0;

  for (const wave of subWaves) {
    if (collected.size >= targetCount) break;

    let taskIdx = 0;
    let consecutiveStallBatches = 0;
    let consecutiveZeroRaw = 0;

    while (collected.size < targetCount && taskIdx < wave.tasks.length) {
      const batch = wave.tasks.slice(taskIdx, taskIdx + CONCURRENCY);
      taskIdx += CONCURRENCY;
      totalQueriesRun += batch.length;
      batchCounter++;

      const batchResponses = await Promise.all(
        batch.map((t, idx) =>
          fetchSearchPageDirectHttp(t.uri, {
            timeoutMs: 14000,
            maxRetries: 2,
            uaIndex: batchCounter * CONCURRENCY + idx,
          }).then((res) => ({ task: t, res }))
        )
      );

      let rawCount = 0;
      let throttledCount = 0;
      let batchAdded = 0;
      const nowIso = new Date().toISOString();

      for (const { task, res } of batchResponses) {
        if (res.throttled) throttledCount++;
        const items = res.items || [];
        rawCount += items.length;
        let addedFromPage = 0;

        for (const item of items) {
          if (collected.size >= targetCount) break;
          if (task.minDiscountPct > 0) {
            if (
              item.discountPct == null ||
              item.discountPct < task.minDiscountPct
            ) {
              continue;
            }
          }
          if (!collected.has(item.pid) && !usedGlobalPids.has(item.pid)) {
            const record = compactProductRecord({
              pid: item.pid,
              category: cat.name,
              title: item.title,
              mrp: item.mrp,
              currentPrice: item.currentPrice,
              averagePrice: item.currentPrice,
              medianPrice: item.currentPrice,
              minPrice: item.currentPrice,
              maxPrice: item.currentPrice,
              url: item.url,
              history: [item.currentPrice],
              lastChecked: nowIso,
            });
            collected.set(item.pid, record);
            globalProducts[item.pid] = record;
            usedGlobalPids.add(item.pid);
            batchAdded++;
            addedFromPage++;
          }
        }

        if (addedFromPage > 0) {
          productiveSearchPages.add(task.uri);
        }
      }

      if (throttledCount >= Math.ceil(batch.length / 2)) {
        await new Promise((r) => setTimeout(r, 2500));
      } else if (rawCount === 0) {
        consecutiveZeroRaw++;
        if (consecutiveZeroRaw >= 2) {
          await new Promise((r) => setTimeout(r, 1500));
        }
      } else {
        consecutiveZeroRaw = 0;
      }

      process.stdout.write(
        `\r   📦 [${cat.name.padEnd(32)}] ${collected.size}/${targetCount} (+${batchAdded}, raw:${rawCount}, wave:${wave.label}, q:${totalQueriesRun})   `
      );

      if (batchAdded < 3) {
        consecutiveStallBatches++;
        if (consecutiveStallBatches >= 3) {
          break;
        }
      } else {
        consecutiveStallBatches = 0;
      }
    }
  }

  const added = collected.size - startCount;
  console.log(
    `\n   ✅ Completed [${cat.name}]: ${collected.size} products (+${added} new)`
  );
  return { skus: [...collected.keys()].slice(0, targetCount), added };
}

async function main() {
  const dataFile = path.join(__dirname, "price-data.json");
  const skuFile = path.join(__dirname, "skus.json");

  const data = loadData(dataFile);
  const skuData = loadSkus(skuFile);

  const initialSkuCount = skuData.skus.length;
  const totalTarget = CATEGORIES.reduce(
    (s, c) => s + (c.target || DEFAULT_TARGET_PER_CATEGORY),
    0
  );

  console.log("═══════════════════════════════════════════════════════════════════");
  console.log(
    `  🛒 Flipkart.com Bulk Direct-HTTP Harvester (${CATEGORIES.length} Categories)`
  );
  console.log(
    `  🎯 Target: ${totalTarget.toLocaleString("en-IN")} unique products (Existing: ${initialSkuCount.toLocaleString("en-IN")})`
  );
  console.log(
    `  ⚡ Engine: Pure Direct HTTP (${CONCURRENCY}x concurrency, 40 SKUs/page, zero browser)`
  );
  console.log("═══════════════════════════════════════════════════════════════════\n");

  const probe = await fetchSearchPageDirectHttp("/search?q=smartphone&page=1", {
    timeoutMs: 12000,
    maxRetries: 1,
  });
  if (probe.status !== 200 || probe.items.length === 0) {
    throw new Error(
      `Flipkart probe failed: status=${probe.status}, items=${probe.items.length}`
    );
  }
  console.log(
    `🌐 Flipkart Direct HTTP probe OK (${probe.items.length} products/page) — starting harvest!\n`
  );

  const usedGlobalPids = new Set();
  for (const list of Object.values(skuData.categories)) {
    for (const pid of list || []) usedGlobalPids.add(pid);
  }
  for (const pid of skuData.skus || []) usedGlobalPids.add(pid);

  const productiveSearchPages = new Set(skuData.searchPages || []);
  let totalAdded = 0;
  const startMs = Date.now();

  for (let i = 0; i < CATEGORIES.length; i++) {
    const cat = CATEGORIES[i];
    const { skus: catSkus, added } = await harvestCategory(
      cat,
      skuData.categories,
      data.products,
      usedGlobalPids,
      productiveSearchPages
    );
    skuData.categories[cat.name] = catSkus;
    totalAdded += added;

    if (added > 0 || i === CATEGORIES.length - 1) {
      skuData.skus = [...new Set(Object.values(skuData.categories).flat())];
      skuData.searchPages = [...productiveSearchPages];
      fs.writeFileSync(skuFile, JSON.stringify(skuData, null, 2), "utf-8");
      data.lastRun = new Date().toISOString();
      data.runCount = (data.runCount || 0) + 1;
      fs.writeFileSync(dataFile, JSON.stringify(data), "utf-8");
      const elapsedSec = ((Date.now() - startMs) / 1000).toFixed(1);
      console.log(
        `   💾 Checkpoint (${elapsedSec}s) — Total unique PIDs: ${skuData.skus.length.toLocaleString("en-IN")} (+${totalAdded.toLocaleString("en-IN")} new | ${skuData.searchPages.length} search pages)`
      );
    }
  }

  const totalSec = ((Date.now() - startMs) / 1000).toFixed(1);
  console.log("\n═══════════════════════════════════════════════════════════════════");
  console.log(
    `  🎉 Flipkart Harvest Complete in ${totalSec}s! Categories: ${CATEGORIES.length} | Unique PIDs: ${skuData.skus.length.toLocaleString("en-IN")} (+${totalAdded.toLocaleString("en-IN")} new) | Search Pages: ${skuData.searchPages.length}`
  );
  console.log("═══════════════════════════════════════════════════════════════════\n");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
