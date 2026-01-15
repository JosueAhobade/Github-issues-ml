import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="GitHub Issue Predictor", layout="wide")

# --- CONNEXION BASE DE DONNEES ---
# (On utilise le cache pour ne pas recharger la DB à chaque clic)
@st.cache_data
def load_data():
    config = {
        'host': '92.113.22.16',  # Votre IP Hostinger
        'user': 'u616324536_big_data', # Vérifiez votre user
        'password': 'BigData2026@', # Remplacez ici
        'database': 'u616324536_big_data', # Vérifiez votre base
        'port': 3306
    }
    try:
        conn = mysql.connector.connect(**config)
        # On charge tout (ou une partie) dans un DataFrame Pandas
        query = """
        SELECT issue_id, repo, language, time_to_close_hours, 
               comments_count, images_count, has_code_snippet, created_at
        FROM github_issues 
        WHERE state = 'closed' 
        LIMIT 50000
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Petit nettoyage
        df['Has Image'] = df['images_count'].apply(lambda x: "Avec Image" if x > 0 else "Sans Image")
        df['Has Code'] = df['has_code_snippet'].apply(lambda x: "Avec Code" if x == 1 else "Sans Code")
        return df
    except Exception as e:
        st.error(f"Erreur de connexion : {e}")
        return pd.DataFrame()

# --- CHARGEMENT DES DONNÉES ---
with st.spinner('Chargement des données depuis MySQL...'):
    df = load_data()

# --- TITRE ET TABS ---
st.title("🚀 GitHub Issues Intelligence")
tab1, tab2 = st.tabs(["📊 Dashboard (Step 1)", "🤖 Prédiction IA (Step 2 & 3)"])

# =========================================================
# ONGLET 1 : LE DASHBOARD (Remplacement de Power BI)
# =========================================================
with tab1:
    if not df.empty:
        # 1. KPIs (Les grosses cartes)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Issues Analysées", f"{len(df):,}")
        col2.metric("Temps Moyen Résolution", f"{df['time_to_close_hours'].mean():.1f} h")
        col3.metric("Repos Scannés", df['repo'].nunique())
        col4.metric("% avec Code", f"{(df['has_code_snippet'].mean()*100):.1f}%")

        st.divider()

        # 2. Graphiques Ligne 1
        c1, c2 = st.columns(2)
        
        with c1:
            st.subheader("⏱️ Vélocité par Langage")
            # On groupe par langage et on fait la moyenne
            avg_time = df.groupby('language')['time_to_close_hours'].mean().sort_values().reset_index()
            fig_bar = px.bar(avg_time, x='language', y='time_to_close_hours', 
                             color='time_to_close_hours', title="Temps moyen de résolution (Heures)")
            st.plotly_chart(fig_bar, use_container_width=True)

        with c2:
            st.subheader("🖼️ Impact des Images")
            # Comparatif Avec/Sans Image
            avg_img = df.groupby('Has Image')['time_to_close_hours'].mean().reset_index()
            fig_img = px.bar(avg_img, x='Has Image', y='time_to_close_hours', color='Has Image',
                             color_discrete_map={"Avec Image": "green", "Sans Image": "red"})
            st.plotly_chart(fig_img, use_container_width=True)

        # 3. Graphique Ligne 2 (Scatter Plot - Discussion vs Temps)
        st.subheader("💬 Corrélation : Discussions vs Temps de résolution")
        # On prend un échantillon de 1000 points pour ne pas alourdir le graphe
        fig_scatter = px.scatter(df.sample(1000), x="comments_count", y="time_to_close_hours", 
                                 color="language", opacity=0.6, log_y=True,
                                 hover_data=['repo'])
        st.plotly_chart(fig_scatter, use_container_width=True)

    else:
        st.warning("Aucune donnée chargée. Vérifiez la connexion DB.")

# =========================================================
# ONGLET 2 : L'INTERFACE IA (Votre futur modèle)
# =========================================================
with tab2:
    st.header("🔮 Prédiction du temps de résolution")
    st.markdown("""
    Copiez-collez le texte d'une issue GitHub ici. 
    Notre modèle (Step 3) analysera le contenu pour prédire sa complexité.
    """)

    # Zone de saisie (Input)
    col_input, col_pred = st.columns([1, 1])

    with col_input:
        issue_title = st.text_input("Titre de l'issue", "Bug: Application crash on startup")
        issue_body = st.text_area("Description du problème", "When I launch the app, it closes immediately...", height=200)
        
        # Paramètres supplémentaires pour l'IA
        has_code = st.checkbox("Contient du code ?")
        has_img = st.checkbox("Contient une image ?")
        
        predict_btn = st.button("Lancer la Prédiction", type="primary")

    # Zone de résultat (Output)
    with col_pred:
        if predict_btn:
            # --- C'EST ICI QUE VOUS CONNECTEREZ VOTRE API / MODÈLE ---
            # Pour l'instant, on simule une réponse (Mock)
            
            st.info("🧠 Analyse en cours par le modèle...")
            import time
            time.sleep(1) # Simulation de calcul
            
            # Simulation de résultat
            predicted_hours = 24 
            complexity = "Moyenne"
            
            st.success("✅ Analyse Terminée !")
            
            # Affichage Joli des résultats
            st.metric("Temps estimé", f"{predicted_hours} Heures")
            st.metric("Complexité", complexity)
            
            st.progress(50, text="Niveau de difficulté")
            
            st.json({
                "features_detected": {
                    "code_blocks": 1 if has_code else 0,
                    "sentiment": "Negative",
                    "keywords": ["crash", "startup"]
                }
            })