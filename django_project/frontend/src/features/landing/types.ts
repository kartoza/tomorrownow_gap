export interface PartnerLogo {
  name: string;
  logo: string;
  website: string;
  mt?: string; // Optional margin-top for spacing
}

export interface ServiceCard {
  title: string;
  items: string[];
  icon: string;
}

export const partners: PartnerLogo[] = [
    { name: 'CGIAR', logo: '/static/images/cgiar.svg', website: 'https://www.cgiar.org' },
    { name: 'ONE ACRE FUND', logo: '/static/images/oneacrefund.svg', website: 'https://oneacrefund.org' },
    { name: 'Regen Organics', logo: '/static/images/regen_organics.svg', website: 'https://www.regenorganics.co', mt: '1.5rem' },
    { name: 'Rhiza Research', logo: '/static/images/rhiza_v5.svg', website: 'https://rhizaresearch.org' },
    { name: 'Salient', logo: '/static/images/salient.svg', website: 'https://www.salientpredictions.com', mt: '0.2rem' },
    { name: 'Tahmo', logo: '/static/images/tahmo.svg', website: 'https://tahmo.org', mt: '-2rem' },
    { name: 'Tomorrow.io', logo: '/static/images/tomorrow.io.svg', website: 'https://www.tomorrow.io', mt: '1rem' },
];

export const aboutPartners: PartnerLogo[] = [
{ name: 'TomorrowNow', logo: '/static/images/tomorrownow.svg', website: 'https://tomorrownow.org' },
{ name: 'Kartoza', logo: '/static/images/kartoza.svg', website: 'https://kartoza.com' },
{ name: 'Gates Foundation', logo: '/static/images/gates_foundation.svg', website: 'https://www.gatesfoundation.org' }
];

export const services: ServiceCard[] = [
    {
        icon: '/static/images/weather.svg',
        title: 'Weather & Climate Data',
        items: [
            'Historical Climate Reanalysis',
            'Long-term Normals',
            'Medium-Range Forecasts',
            'Seasonal Forecasts',
            'Quality-Controlled Ground Observations',
        ]
    },
    {
        icon: '/static/images/validation.svg',
        title: 'Validation & Localization Suite',
        items: [
            'Forecast Validation Dashboard',
            'Forecast Modeling Benchmarking',
            'Ground Statino QA/QC Tools',
            'Digital Advisory Evaluation Tools'
        ]
    },
    {
        icon: '/static/images/insight.svg',
        title: 'AgroMet Insights',
        items: [
            'Planting Window Advisories',
            'Pest & Disease Risk Alerts',
            'Crop Variety Assessment',
            'Digital Crop Advisory Service',
            'Digital Weather Advisories Service'
        ]
    }
];
