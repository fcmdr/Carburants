"""
Carburants France - Streamlit Dashboard
Displays cheapest fuel stations from Snowflake data warehouse
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector

# Load environment variables from .env file (for local development)
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(env_path, override=True)

# Page config
st.set_page_config(
    page_title="Carburants France",
    page_icon="\u26fd",
    layout="wide"
)


def get_secret(key: str) -> str:
    """
    Get a secret value from Streamlit Cloud secrets or environment variables.
    Streamlit Cloud: uses st.secrets (from .streamlit/secrets.toml or dashboard)
    Local development: uses .env file via os.environ
    """
    # Try Streamlit secrets first (for Streamlit Cloud deployment)
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return st.secrets[key]
    except Exception:
        # st.secrets raises error if no secrets.toml exists - that's fine for local dev
        pass
    # Fall back to environment variables (for local development)
    value = os.environ.get(key)
    if value is None:
        raise KeyError(f"Missing required secret: {key}. "
                       f"Set it in .env (local) or Streamlit Cloud secrets.")
    return value


# Cache the Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    """Create Snowflake connection using secrets (Streamlit Cloud or .env)."""
    return snowflake.connector.connect(
        account=get_secret("SNOWFLAKE_ACCOUNT"),
        user=get_secret("SNOWFLAKE_USER"),
        password=get_secret("SNOWFLAKE_PASSWORD"),
        database=get_secret("SNOWFLAKE_DATABASE"),
        warehouse=get_secret("SNOWFLAKE_WAREHOUSE"),
        role=get_secret("SNOWFLAKE_ROLE"),
    )

def run_query(query):
    """Execute a query and return results as DataFrame."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0].lower() for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)

@st.cache_data(ttl=300)
def load_cheapest_stations():
    """Load cheapest stations report from Snowflake."""
    query = """
    SELECT
        station_id, ville, adresse, code_postal, code_departement, region,
        type_station, is_automate_24_24, carburant_nom, prix_euros,
        categorie_prix, tendance_prix, rang_national, rang_region,
        is_top10_france, is_top10_region, latitude, longitude, date_prix
    FROM PUBLIC_MARTS.RPT_STATIONS_MOINS_CHERES
    ORDER BY carburant_nom, rang_national
    """
    return run_query(query)

@st.cache_data(ttl=300)
def load_regional_prices():
    """Load regional average prices from Snowflake."""
    query = """
    SELECT *
    FROM PUBLIC_MARTS.RPT_PRIX_MOYEN_REGION
    WHERE is_latest = TRUE
    ORDER BY carburant_nom, rang_region
    """
    return run_query(query)

@st.cache_data(ttl=300)
def get_fuel_types():
    """Get list of available fuel types."""
    query = "SELECT DISTINCT carburant_nom FROM PUBLIC_MARTS.DIM_CARBURANTS ORDER BY carburant_nom"
    return run_query(query)["carburant_nom"].tolist()

@st.cache_data(ttl=300)
def search_stations(city_name=None, dept_code=None, fuel_type="GAZOLE"):
    """Search stations by city or department."""
    where_clauses = [
        f"f.carburant_nom = '{fuel_type}'",
        f"f.date_prix = (SELECT MAX(date_prix) FROM PUBLIC_MARTS.FCT_PRIX_CARBURANTS WHERE carburant_nom = '{fuel_type}')"
    ]

    if city_name:
        where_clauses.append(f"LOWER(s.ville) LIKE LOWER('%{city_name}%')")
    if dept_code:
        where_clauses.append(f"s.code_departement = '{dept_code}'")

    query = f"""
    SELECT
        f.station_id, s.ville, s.adresse, s.code_postal, s.code_departement,
        s.region, s.type_station, s.is_automate_24_24, f.carburant_nom,
        f.prix_euros, f.categorie_prix, f.tendance_prix, f.rang_national,
        f.rang_region, s.latitude, s.longitude, f.date_prix
    FROM PUBLIC_MARTS.FCT_PRIX_CARBURANTS f
    JOIN PUBLIC_MARTS.DIM_STATIONS s ON f.station_id = s.station_id
    WHERE {' AND '.join(where_clauses)}
    ORDER BY f.prix_euros ASC
    LIMIT 500
    """
    return run_query(query)

# Title
st.title("\u26fd Carburants France - Prix des Carburants")
st.markdown("*Trouvez les stations-service les moins cheres pres de chez vous*")

# Tabs for different views
tab_top, tab_search = st.tabs(["Top Stations France", "Recherche par Ville/Departement"])

# ============ TAB 1: TOP STATIONS ============
with tab_top:
    try:
        df = load_cheapest_stations()

        if df.empty:
            st.warning("Aucune donnee trouvee. Executez d'abord dbt build.")
            st.stop()

        # Sidebar filters
        st.sidebar.header("Filtres - Top Stations")
        fuel_types = sorted(df["carburant_nom"].unique())
        selected_fuel = st.sidebar.selectbox(
            "Type de carburant",
            options=fuel_types,
            index=fuel_types.index("GAZOLE") if "GAZOLE" in fuel_types else 0,
            key="top_fuel"
        )

        regions = ["Toutes les regions"] + sorted(df["region"].dropna().unique().tolist())
        selected_region = st.sidebar.selectbox("Region", options=regions, key="top_region")

        top_n = st.sidebar.slider("Nombre de stations", min_value=10, max_value=100, value=20, key="top_n")

        # Filter data
        filtered_df = df[df["carburant_nom"] == selected_fuel].copy()
        if selected_region != "Toutes les regions":
            filtered_df = filtered_df[filtered_df["region"] == selected_region]
        filtered_df = filtered_df.head(top_n)

        # KPIs
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Prix minimum", f"{filtered_df['prix_euros'].min():.3f} EUR/L")
        with col2:
            st.metric("Prix maximum", f"{filtered_df['prix_euros'].max():.3f} EUR/L")
        with col3:
            st.metric("Prix moyen", f"{filtered_df['prix_euros'].mean():.3f} EUR/L")
        with col4:
            st.metric("Stations affichees", len(filtered_df))

        # Table and map
        col_table, col_map = st.columns([1, 1])

        with col_table:
            st.subheader(f"Top {top_n} stations - {selected_fuel}")
            display_df = filtered_df[["rang_national", "ville", "adresse", "code_postal", "prix_euros", "tendance_prix", "type_station"]].copy()
            display_df.columns = ["Rang", "Ville", "Adresse", "CP", "Prix (EUR)", "Tendance", "Type"]
            st.dataframe(display_df, hide_index=True, use_container_width=True, height=400)

        with col_map:
            st.subheader("Carte des stations")
            map_df = filtered_df.dropna(subset=["latitude", "longitude"])
            if not map_df.empty:
                fig = px.scatter_mapbox(
                    map_df, lat="latitude", lon="longitude", color="prix_euros",
                    zoom=5, hover_name="ville",
                    hover_data={"adresse": True, "prix_euros": ":.3f", "rang_national": True, "latitude": False, "longitude": False},
                    color_continuous_scale="RdYlGn_r", labels={"prix_euros": "Prix (EUR)"}
                )
                fig.update_layout(mapbox_style="open-street-map", margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=400)
                st.plotly_chart(fig, use_container_width=True)

        # Footer
        st.markdown("---")
        st.caption(f"Donnees du: {filtered_df['date_prix'].iloc[0]}")

    except Exception as e:
        st.error(f"Erreur: {str(e)}")

# ============ TAB 2: SEARCH ============
with tab_search:
    st.subheader("Rechercher des stations")

    col_search1, col_search2, col_search3 = st.columns([2, 1, 1])

    with col_search1:
        search_city = st.text_input("Ville (ex: Issy-les-Moulineaux, Paris, Lyon...)", key="search_city")

    with col_search2:
        search_dept = st.text_input("Code departement (ex: 92, 75, 69...)", key="search_dept")

    with col_search3:
        try:
            fuel_list = get_fuel_types()
        except:
            fuel_list = ["GAZOLE", "SP95", "SP98", "E10", "E85", "GPLC"]
        search_fuel = st.selectbox("Carburant", options=fuel_list, index=0, key="search_fuel")

    if st.button("Rechercher", type="primary"):
        if not search_city and not search_dept:
            st.warning("Veuillez entrer une ville ou un code departement")
        else:
            with st.spinner("Recherche en cours..."):
                try:
                    results = search_stations(
                        city_name=search_city if search_city else None,
                        dept_code=search_dept if search_dept else None,
                        fuel_type=search_fuel
                    )

                    if results.empty:
                        st.warning("Aucune station trouvee pour ces criteres.")
                    else:
                        st.success(f"{len(results)} station(s) trouvee(s)")

                        # KPIs
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Prix minimum", f"{results['prix_euros'].min():.3f} EUR/L")
                        with col2:
                            st.metric("Prix maximum", f"{results['prix_euros'].max():.3f} EUR/L")
                        with col3:
                            st.metric("Prix moyen", f"{results['prix_euros'].mean():.3f} EUR/L")

                        # Table
                        st.subheader(f"Stations - {search_fuel}")
                        display_results = results[["ville", "adresse", "code_postal", "prix_euros", "rang_national", "rang_region", "type_station"]].copy()
                        display_results.columns = ["Ville", "Adresse", "CP", "Prix (EUR)", "Rang National", "Rang Region", "Type"]
                        st.dataframe(display_results, hide_index=True, use_container_width=True)

                        # Map
                        map_results = results.dropna(subset=["latitude", "longitude"])
                        if not map_results.empty:
                            st.subheader("Carte")
                            fig = px.scatter_mapbox(
                                map_results, lat="latitude", lon="longitude", color="prix_euros",
                                zoom=11, hover_name="ville",
                                hover_data={"adresse": True, "prix_euros": ":.3f", "latitude": False, "longitude": False},
                                color_continuous_scale="RdYlGn_r", labels={"prix_euros": "Prix (EUR)"}
                            )
                            fig.update_layout(mapbox_style="open-street-map", margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=400)
                            st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.error(f"Erreur lors de la recherche: {str(e)}")

# Sidebar diagnostic
with st.sidebar.expander("Diagnostic"):
    if st.button("Verifier les tables"):
        try:
            query = """
            SELECT table_schema, table_name, row_count
            FROM information_schema.tables
            WHERE table_schema LIKE '%MART%' OR table_schema = 'RAW'
            ORDER BY table_schema, table_name
            """
            st.dataframe(run_query(query), use_container_width=True)
        except Exception as e:
            st.error(f"Erreur: {e}")

    dept_check = st.text_input("Voir villes du departement:", key="dept_check")
    if dept_check and st.button("Afficher villes"):
        try:
            query = f"""
            SELECT DISTINCT ville, COUNT(*) as nb_stations
            FROM PUBLIC_MARTS.DIM_STATIONS
            WHERE code_departement = '{dept_check}'
            GROUP BY ville
            ORDER BY nb_stations DESC
            LIMIT 50
            """
            st.dataframe(run_query(query), use_container_width=True)
        except Exception as e:
            st.error(f"Erreur: {e}")

    st.markdown("---")
    st.markdown("**Distribution carburants par dept**")
    fuel_dept_check = st.text_input("Code departement:", key="fuel_dept_check")
    if fuel_dept_check and st.button("Analyser distribution"):
        try:
            # Query 1: Fuel distribution for the department
            query_dist = f"""
            SELECT
                f.carburant_nom,
                COUNT(*) as nb_prix,
                COUNT(DISTINCT f.station_id) as nb_stations,
                MIN(f.prix_euros) as prix_min,
                MAX(f.prix_euros) as prix_max,
                AVG(f.prix_euros) as prix_moyen
            FROM PUBLIC_MARTS.FCT_PRIX_CARBURANTS f
            JOIN PUBLIC_MARTS.DIM_STATIONS s ON f.station_id = s.station_id
            WHERE s.code_departement = '{fuel_dept_check}'
            GROUP BY f.carburant_nom
            ORDER BY nb_stations DESC
            """
            st.markdown("**Stations par type de carburant:**")
            st.dataframe(run_query(query_dist), use_container_width=True)

            # Query 2: Date range per fuel type for the department
            query_dates = f"""
            SELECT
                f.carburant_nom,
                MIN(f.date_prix) as date_min,
                MAX(f.date_prix) as date_max,
                COUNT(DISTINCT f.date_prix) as nb_jours
            FROM PUBLIC_MARTS.FCT_PRIX_CARBURANTS f
            JOIN PUBLIC_MARTS.DIM_STATIONS s ON f.station_id = s.station_id
            WHERE s.code_departement = '{fuel_dept_check}'
            GROUP BY f.carburant_nom
            ORDER BY f.carburant_nom
            """
            st.markdown("**Plage de dates par carburant:**")
            st.dataframe(run_query(query_dates), use_container_width=True)

            # Query 3: Latest date comparison (to see if filtering issue)
            query_latest = f"""
            SELECT
                carburant_nom,
                MAX(date_prix) as derniere_date,
                (SELECT MAX(date_prix) FROM PUBLIC_MARTS.FCT_PRIX_CARBURANTS) as date_max_globale
            FROM PUBLIC_MARTS.FCT_PRIX_CARBURANTS
            GROUP BY carburant_nom
            ORDER BY carburant_nom
            """
            st.markdown("**Dernieres dates par carburant (global):**")
            st.dataframe(run_query(query_latest), use_container_width=True)

        except Exception as e:
            st.error(f"Erreur: {e}")
