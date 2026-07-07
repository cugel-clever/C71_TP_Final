```mermaid

flowchart TD
    style all fill:#f2f2f2,stroke:#888,stroke-width:1px

    A[Start] --> B{Définir les paramètres}
    B --> C[API Key]
    C --> D[MongoDB URI]
    D --> E[Annee : 2003]
    E --> F[Terme de recherche : love]

    F --> G["Appeler get_details_requete_films(param_recherche, cle_api)"]
    G --> H{Résultats valides?}
    H -- Oui --> I[Sauvegarder dans MongoDB]
    I --> J[Extraire données depuis MongoDB]
    
    H -- Non --> K[Afficher erreur]
    K --> L[Sortir]

    J --> M{Nettoyer les données}
    M -- Oui --> N[Manipuler avec pandas et matplotlib]
    M -- Non --> O[Sortir]

    L --> P[Fin]
    N --> P

    style B fill:#e6e6fa,stroke:#c0c0c0,stroke-width:1px
    style C fill:#f8f8ff,stroke:#d3d3d3,stroke-width:1px
    style E fill:#ffe4b5,stroke:#cd853f,stroke-width:1px
    style F fill:#e6ffff,stroke:#add8e6,stroke-width:1px

    style G fill:#ffffff,stroke:#808080,stroke-width:1px
    style H fill:#d4edda,stroke:#98dbc6,stroke-width:1px
    style I fill:#d9edf7,stroke:#bce8f1,stroke-width:1px
    style J fill:#ffffff,stroke:#808080,stroke-width:1px

    style K fill:#e3e3e3,stroke:#a9a9a9,stroke-width:1px
    style L fill:#ffffff,stroke:#808080,stroke-width:1px
    style P fill:#ffffff,stroke:#808080,stroke-width:1px

    style M fill:#f5f5f5,stroke:#b2b2b2,stroke-width:1px
    style N fill:#ffffff,stroke:#808080,stroke-width:1px
